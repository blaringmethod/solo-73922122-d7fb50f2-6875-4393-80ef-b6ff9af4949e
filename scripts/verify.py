#!/usr/bin/env python3
"""端到端行为验证（stdlib 实现）。

前置：代理在 :8000、上游已启动（scripts/run-local.sh 或 compose）。
注意：compose 交付中上游不暴露宿主端口，因此本脚本对上游的管理操作
一律经代理透传（/proxy/upstream/*），只需代理地址。

覆盖：
  缓存代理（既有）：
  1. 匿名 MISS -> HIT，且上游只收到一次请求
  2. 缓存键按 Accept-Language 区分
  3. Cookie / Authorization 直连绕过缓存、不参与合并
  4. personal（Set-Cookie + private,no-store）不缓存（模式经审批流切换）
  5. 500 失败不缓存且结束所有在途等待
  6. 同键并发未命中只打一次上游；等待者断开不影响其他人
  7. 过期不复用
  8. 按公告清除全部语言；旧在途不回填；清除后不复用旧在途
  9. 各种非法 Cache-Control 不缓存
  双人审批（新增）：
 10. Basic 认证、固定双账号、发起人不能批准、并发批准只生效一次
 11. 提案提交生效、上游修订前进、实际配置可查
 12. 旧修订拒绝；配置/重置入口不得绕过审批
 13. 丢回执 -> 待核实（不报成功/失败）-> 闸关闭不放行下一笔 -> 核实收敛
 14. 重试原提案：上游按提案 ID 去重，返回原回执，不重复生效
 15. 网络等待期间数据库可写（不持写锁）
"""
from __future__ import annotations

import base64
import http.client
import json
import os
import sys
import threading
import time
import urllib.request
import urllib.error

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PROXY = os.environ.get("PROXY_ADDR", "127.0.0.1:8000")
OPS_A = os.environ.get("VERIFY_OPS_A", "alice:alice-pass")
OPS_B = os.environ.get("VERIFY_OPS_B", "bob:bob-pass")

PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name}  {detail}")


def raw_request(host_port: str, method: str, path: str, headers=None, timeout=20):
    conn = http.client.HTTPConnection(host_port, timeout=timeout)
    conn.request(method, path, headers=headers or {})
    resp = conn.getresponse()
    body = resp.read()
    hs = {k.lower(): v for k, v in resp.getheaders()}
    return resp.status, hs, body, conn


def proxy_get(path, headers=None, timeout=20):
    status, hs, body, conn = raw_request(PROXY, "GET", path, headers, timeout)
    conn.close()
    return status, hs, body


def proxy_post(path: str, payload: dict | None = None, timeout=20):
    data = json.dumps(payload).encode() if payload is not None else b""
    req = urllib.request.Request(
        f"http://{PROXY}{path}", data=data,
        headers={"content-type": "application/json"}, method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        body = e.read()
        try:
            return e.code, json.loads(body)
        except ValueError:
            return e.code, {"raw": body.decode(errors="replace")}


def approval(user_pass: str | None, method: str, path: str,
             payload: dict | None = None, timeout=30):
    """调用审批 API；user_pass=None 表示不带凭证。"""
    headers = {"content-type": "application/json"}
    if user_pass is not None:
        token = base64.b64encode(user_pass.encode()).decode()
        headers["authorization"] = f"Basic {token}"
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        f"http://{PROXY}{path}", data=data, headers=headers, method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        body = e.read()
        try:
            return e.code, json.loads(body)
        except ValueError:
            return e.code, {"raw": body.decode(errors="replace")}


def admin_reset() -> None:
    proxy_post("/proxy/upstream/reset")
    proxy_post("/proxy/reset")
    proxy_post("/proxy/upstream/chaos", {"drop_receipt": False})


def upstream_count() -> int:
    with urllib.request.urlopen(
        f"http://{PROXY}/proxy/upstream/state", timeout=10
    ) as r:
        return json.loads(r.read())["counters"]["total_requests"]


def config_state() -> dict:
    with urllib.request.urlopen(
        f"http://{PROXY}/proxy/upstream/config-state", timeout=10
    ) as r:
        return json.loads(r.read())


def approval_state(user: str = OPS_A) -> dict:
    status, data = approval(user, "GET", "/approval/api/state")
    assert status == 200, f"approval state -> {status}"
    return data


def set_profile_via_approval(notice_id: str, profile: str) -> dict:
    """完整走一遍双人审批：A 发起 -> B 批准 -> 提交 -> 断言已生效。"""
    rev = config_state()["revision"]
    status, data = approval(OPS_A, "POST", "/approval/api/proposals", {
        "notice_id": notice_id, "target_profile": profile,
        "base_revision": rev,
    })
    assert status == 201, f"create -> {status}: {data}"
    pid = data["proposal"]["proposal_id"]
    status, data = approval(OPS_B, "POST",
                            f"/approval/api/proposals/{pid}/approve")
    assert status == 200, f"approve -> {status}: {data}"
    status, data = approval(OPS_A, "POST",
                            f"/approval/api/proposals/{pid}/submit")
    assert status == 200 and data["proposal"]["status"] == "APPLIED", \
        f"submit -> {status}: {data}"
    return data["proposal"]


def ensure_profile(notice_id: str, profile: str) -> None:
    cur = config_state()
    if cur["overrides"].get(notice_id, cur["default_profile"]) != profile:
        set_profile_via_approval(notice_id, profile)


def section(t: str) -> None:
    print(f"\n=== {t} ===")


def main() -> int:
    section("0. 重置环境（演示旋钮；受控模式配置不受影响）")
    admin_reset()
    proxy_post("/proxy/upstream/config",
               {"latency_ms": 100, "max_age": 30})
    ensure_profile("1", "public")
    ensure_profile("2", "public")
    ensure_profile("3", "public")
    check("healthz", proxy_get("/healthz")[0] == 200)

    section("1. 匿名 MISS -> HIT，上游仅一次请求")
    n0 = upstream_count()
    s1, h1, b1 = proxy_get("/notices/1", {"Accept-Language": "zh-CN"})
    s2, h2, _ = proxy_get("/notices/1", {"Accept-Language": "zh-CN"})
    check("首次 MISS", h1.get("x-cache") == "MISS", h1.get("x-cache"))
    check("二次 HIT", h2.get("x-cache") == "HIT", h2.get("x-cache"))
    check("两次仅一次上游请求", upstream_count() - n0 == 1,
          f"delta={upstream_count() - n0}")
    check("HIT 响应 Age 头存在", h2.get("age") is not None)

    section("2. 缓存键按 Accept-Language 区分")
    n0 = upstream_count()
    s3, h3, b3 = proxy_get("/notices/1", {"Accept-Language": "en-US"})
    check("新语言 MISS", h3.get("x-cache") == "MISS", h3.get("x-cache"))
    zh = json.loads(b1)["language"]
    en = json.loads(b3)["language"]
    check("语言内容不同", zh != en, f"{zh} vs {en}")
    s4, h4, _ = proxy_get("/notices/1", {"Accept-Language": "en-US"})
    check("新语言二次 HIT", h4.get("x-cache") == "HIT")
    check("两种语言各打一次上游", upstream_count() - n0 == 1)

    section("3. Cookie / Authorization 直连，绕过缓存且不污染缓存")
    n0 = upstream_count()
    s5, h5, _ = proxy_get("/notices/1",
                          {"Accept-Language": "zh-CN", "Cookie": "SID=u1"})
    check("Cookie 请求 BYPASS", h5.get("x-cache") == "BYPASS", h5.get("x-cache"))
    s6, h6, _ = proxy_get(
        "/notices/1",
        {"Accept-Language": "zh-CN", "Authorization": "Bearer t"},
    )
    check("Auth 请求 BYPASS", h6.get("x-cache") == "BYPASS", h6.get("x-cache"))
    check("两次绕过都打了上游", upstream_count() - n0 == 2)
    s7, h7, _ = proxy_get("/notices/1", {"Accept-Language": "zh-CN"})
    check("绕过后匿名仍 HIT（缓存未被污染）", h7.get("x-cache") == "HIT",
          h7.get("x-cache"))

    section("4. personal 响应（Set-Cookie + private,no-store）不缓存")
    set_profile_via_approval("2", "personal")
    n0 = upstream_count()
    s8, h8, _ = proxy_get("/notices/2", {"Accept-Language": "zh-CN"})
    check("personal 首次 MISS-NO-CACHE",
          h8.get("x-cache") == "MISS-NO-CACHE", h8.get("x-cache"))
    check("personal 响应带回 Set-Cookie", "set-cookie" in h8)
    s9, h9, _ = proxy_get("/notices/2", {"Accept-Language": "zh-CN"})
    check("personal 再次仍是 MISS-NO-CACHE",
          h9.get("x-cache") == "MISS-NO-CACHE", h9.get("x-cache"))
    check("每次都打上游（无缓存）", upstream_count() - n0 == 2)
    set_profile_via_approval("2", "public")

    section("5. 上游 500：不缓存，所有并发等待者都收到失败")
    proxy_post("/proxy/upstream/config",
               {"notice_id": "3", "failure": "error500", "latency_ms": 300})
    results = []
    threads = [
        threading.Thread(target=lambda: results.append(
            proxy_get("/notices/3", {"Accept-Language": "zh-CN"})))
        for _ in range(4)
    ]
    for t in threads: t.start()
    for t in threads: t.join()
    check("4 个并发全部 502", all(r[0] == 502 for r in results),
          str([r[0] for r in results]))
    check("全部标记 ERROR", all(r[1].get("x-cache") == "ERROR" for r in results))
    s10, h10, _ = proxy_get("/notices/3", {"Accept-Language": "zh-CN"})
    check("失败后再次请求仍重新打上游（未缓存失败）",
          h10.get("x-cache") == "ERROR")
    proxy_post("/proxy/upstream/config",
               {"notice_id": "3", "failure": "none", "latency_ms": 100})

    section("6. 同键并发未命中只请求上游一次")
    proxy_post("/proxy/upstream/config",
               {"notice_id": "1", "latency_ms": 1500, "max_age": 30})
    proxy_post("/proxy/purge/1")
    n0 = upstream_count()
    conc: list = []
    threads = [
        threading.Thread(target=lambda: conc.append(
            proxy_get("/notices/1", {"Accept-Language": "fr-FR"}, timeout=20)))
        for _ in range(6)
    ]
    t0 = time.monotonic()
    for t in threads: t.start()
    for t in threads: t.join()
    elapsed = time.monotonic() - t0
    tags = [r[1].get("x-cache") for r in conc]
    check("6 个请求全部 200", all(r[0] == 200 for r in conc))
    check("恰好 1 个 MISS + 5 个 MISS-WAIT",
          sorted(tags).count("MISS") == 1 and tags.count("MISS-WAIT") == 5,
          str(tags))
    check("上游只收到 1 次请求", upstream_count() - n0 == 1,
          f"delta={upstream_count() - n0}")
    check("总耗时≈单次延迟而非 6 倍", elapsed < 3.5, f"{elapsed:.2f}s")
    rids = {r[1].get("x-upstream-request-id") for r in conc}
    check("所有等待者拿到同一上游响应", len(rids) == 1, str(rids))

    section("7. 等待者断开不影响其他等待者")
    proxy_post("/proxy/upstream/config", {"notice_id": "1", "latency_ms": 2000})
    proxy_post("/proxy/purge/1")
    got: list = []

    def slow_waiter():
        try:
            got.append(proxy_get("/notices/1",
                                 {"Accept-Language": "de-DE"}, timeout=20))
        except Exception as e:  # noqa: BLE001
            got.append(("ERR", {}, str(e).encode()))

    def disconnecting_waiter():
        try:
            proxy_get("/notices/1", {"Accept-Language": "de-DE"}, timeout=0.4)
        except Exception:
            pass

    t_a = threading.Thread(target=slow_waiter)
    t_b = threading.Thread(target=disconnecting_waiter)
    t_a.start(); time.sleep(0.15); t_b.start()
    t_b.join(); t_a.join()
    check("断开发生后其他等待者仍拿到 200",
          len(got) == 1 and got[0][0] == 200, str(got[:1]))
    n0 = upstream_count()
    _, h_after, _ = proxy_get("/notices/1", {"Accept-Language": "de-DE"})
    check("leader 结果已正常回填（后续 HIT）",
          h_after.get("x-cache") == "HIT", h_after.get("x-cache"))
    check("回填验证未再打上游", upstream_count() - n0 == 0)

    section("8. 过期不复用")
    proxy_post("/proxy/upstream/config",
               {"notice_id": "1", "latency_ms": 50, "max_age": 2})
    proxy_post("/proxy/purge/1")
    _, h_a, _ = proxy_get("/notices/1", {"Accept-Language": "it-IT"})
    check("短 max-age 首次 MISS", h_a.get("x-cache") == "MISS")
    _, h_b, _ = proxy_get("/notices/1", {"Accept-Language": "it-IT"})
    check("未过期 HIT", h_b.get("x-cache") == "HIT", h_b.get("x-cache"))
    time.sleep(2.3)
    n0 = upstream_count()
    _, h_c, _ = proxy_get("/notices/1", {"Accept-Language": "it-IT"})
    check("过期后重新 MISS（不复用）", h_c.get("x-cache") == "MISS",
          h_c.get("x-cache"))
    check("过期请求确实打了上游", upstream_count() - n0 == 1)

    section("9. 按公告清除：全语言失效 + 旧在途不回填 + 不复用旧在途")
    proxy_post("/proxy/upstream/config",
               {"notice_id": "1", "latency_ms": 2500, "max_age": 30})
    proxy_get("/notices/1", {"Accept-Language": "zh-CN"})
    proxy_get("/notices/1", {"Accept-Language": "en-US"})
    st = json.loads(proxy_get("/proxy/state")[2])
    cached_langs = {c["accept_language"] for c in st["cache"]
                    if c["notice_id"] == "1"}
    check("清除前两种语言均已缓存",
          {"zh-CN", "en-US"}.issubset(cached_langs), str(cached_langs))
    box: list = []
    threads = [
        threading.Thread(target=lambda: box.append(
            proxy_get("/notices/1", {"Accept-Language": "ja-JP"}, timeout=20)))
        for _ in range(3)
    ]
    for t in threads: t.start()
    time.sleep(0.4)
    st_mid = json.loads(proxy_get("/proxy/state")[2])
    check("清除前存在 1 个在途单元",
          any(i["notice_id"] == "1" for i in st_mid["inflight"]))
    _, purge = proxy_post("/proxy/purge/1")
    check("清除接口报告全语言缓存条目删除",
          purge["purged_cache_entries"] >= 2, str(purge))
    check("清除接口标记了在途单元", purge["marked_inflight"] >= 1, str(purge))
    for t in threads: t.join()
    check("在途等待者均拿到当时响应（200）",
          all(r[0] == 200 for r in box), str([r[0] for r in box]))
    time.sleep(0.2)
    st_after = json.loads(proxy_get("/proxy/state")[2])
    check("旧在途完成后未回填缓存",
          all(c["notice_id"] != "1" for c in st_after["cache"]),
          str(st_after["cache"]))
    n0 = upstream_count()
    _, h_new, _ = proxy_get("/notices/1", {"Accept-Language": "ja-JP"})
    check("清除后新请求重新 MISS（不复用旧在途）",
          h_new.get("x-cache") == "MISS", h_new.get("x-cache"))
    check("新请求确实重新打了上游", upstream_count() - n0 == 1)
    _, h_zh, _ = proxy_get("/notices/1", {"Accept-Language": "zh-CN"})
    check("旧语言缓存同样已失效", h_zh.get("x-cache") == "MISS",
          h_zh.get("x-cache"))
    proxy_post("/proxy/upstream/config",
               {"notice_id": "1", "latency_ms": 100, "max_age": 30})

    section("10. 非法 Cache-Control 不缓存（经上游自定义头）")
    bad_headers = [
        "max-age=30",                       # 无 public
        "public",                           # 无 max-age
        "public, max-age=-5",               # 负数
        "public, max-age=1.5",              # 小数
        "public, max-age=abc",              # 非数字
        "public, max-age=30, no-store",     # no-store
        "public, max-age=30, private",      # private
        "public, max-age=",                 # 空值
    ]
    for i, cc in enumerate(bad_headers):
        nid = str(100 + i)
        proxy_post("/proxy/upstream/config",
                   {"notice_id": nid, "cache_control": cc, "latency_ms": 0})
        _, hh1, _ = proxy_get(f"/notices/{nid}", {"Accept-Language": "zh-CN"})
        _, hh2, _ = proxy_get(f"/notices/{nid}", {"Accept-Language": "zh-CN"})
        check(f"不缓存 [{cc}]",
              hh1.get("x-cache") == "MISS-NO-CACHE"
              and hh2.get("x-cache") == "MISS-NO-CACHE",
              f"{hh1.get('x-cache')} / {hh2.get('x-cache')}")
    _, h404, _ = proxy_get("/notices/404-missing",
                           {"Accept-Language": "zh-CN"})
    check("404 不缓存", h404.get("x-cache") == "MISS-NO-CACHE",
          h404.get("x-cache"))

    # ================= 双人审批 =================

    section("11. 审批页认证：HTTP Basic + 启动配置固定双账号")
    status, _ = approval(None, "GET", "/approval/api/state")
    check("无凭证 401", status == 401, str(status))
    status, _ = approval("alice:wrong-pass", "GET", "/approval/api/state")
    check("错误口令 401", status == 401, str(status))
    status, data = approval(OPS_A, "GET", "/approval/api/state")
    check("正确凭证 200 且标识本人", status == 200 and data["me"] == "alice",
          f"{status} {data.get('me') if data else None}")
    check("账号恰为启动配置的两个",
          data["accounts"] == ["alice", "bob"], str(data["accounts"]))
    page_status, _, _, page_conn = raw_request(PROXY, "GET", "/approval")
    page_conn.close()
    check("审批页无凭证同样 401", page_status == 401, str(page_status))

    section("12. 提案生命周期：发起 -> 批准 -> 提交 -> 生效")
    rev0 = config_state()["revision"]
    status, data = approval(OPS_A, "POST", "/approval/api/proposals", {
        "notice_id": "900", "target_profile": "personal",
        "base_revision": rev0,
    })
    check("发起提案 201", status == 201, f"{status}: {data}")
    pid = data["proposal"]["proposal_id"]
    check("提案含公告/目标模式/所见修订/发起人",
          data["proposal"]["notice_id"] == "900"
          and data["proposal"]["target_profile"] == "personal"
          and data["proposal"]["base_revision"] == rev0
          and data["proposal"]["proposer"] == "alice")
    check("初始状态待批准", data["proposal"]["status"] == "PENDING")
    status, _ = approval(OPS_A, "PUT", f"/approval/api/proposals/{pid}",
                         {"target_profile": "public"})
    check("提案不可修改（无修改入口）", status in (404, 405), str(status))
    status, _ = approval(OPS_A, "POST",
                         f"/approval/api/proposals/{pid}/approve")
    check("发起人批准自己 -> 403", status == 403, str(status))
    # 并发批准：只有一笔生效
    approve_results: list[int] = []

    def do_approve():
        s, _ = approval(OPS_B, "POST",
                        f"/approval/api/proposals/{pid}/approve")
        approve_results.append(s)

    ths = [threading.Thread(target=do_approve) for _ in range(6)]
    for t in ths: t.start()
    for t in ths: t.join()
    check("并发批准恰好一次 200",
          approve_results.count(200) == 1
          and all(s == 409 for s in approve_results if s != 200),
          str(approve_results))
    status, data = approval(OPS_A, "POST",
                            f"/approval/api/proposals/{pid}/submit")
    check("提交后已生效", status == 200
          and data["proposal"]["status"] == "APPLIED",
          f"{status}: {data.get('proposal', {}).get('status')}")
    check("回执已随提案持久化",
          data["proposal"]["receipt"]["outcome"] == "applied"
          and data["proposal"]["receipt"]["new_revision"] == rev0 + 1)
    cur = config_state()
    check("上游修订前进且实际配置可查",
          cur["revision"] == rev0 + 1
          and cur["overrides"].get("900") == "personal",
          f"rev={cur['revision']} overrides={cur['overrides']}")

    section("13. 旧修订拒绝；配置/重置入口不得绕过审批")
    rev1 = config_state()["revision"]
    # 同一修订发起两笔；第一笔生效后，第二笔即基于旧修订
    stale = []
    for nid, tgt in (("901", "personal"), ("902", "personal")):
        s, d = approval(OPS_A, "POST", "/approval/api/proposals", {
            "notice_id": nid, "target_profile": tgt, "base_revision": rev1,
        })
        pid2 = d["proposal"]["proposal_id"]
        approval(OPS_B, "POST", f"/approval/api/proposals/{pid2}/approve")
        stale.append(pid2)
    s, d = approval(OPS_A, "POST",
                    f"/approval/api/proposals/{stale[0]}/submit")
    check("第一笔生效", d["proposal"]["status"] == "APPLIED",
          d["proposal"]["status"])
    s, d = approval(OPS_A, "POST",
                    f"/approval/api/proposals/{stale[1]}/submit")
    check("第二笔旧修订被拒绝",
          d["proposal"]["status"] == "REJECTED"
          and d["proposal"]["receipt"]["reason"] == "stale_revision",
          f"{d['proposal']['status']} {d['proposal'].get('receipt')}")
    check("拒绝回执同样持久保存（修订未再前进）",
          config_state()["revision"] == rev1 + 1,
          str(config_state()["revision"]))
    # 原配置入口不得绕过审批
    s, d = proxy_post("/proxy/upstream/config",
                      {"notice_id": "900", "profile": "public"})
    check("/admin/config 拒绝 profile（不得绕过审批）", s == 400, f"{s}: {d}")
    # 重置入口不得改动受控模式配置
    proxy_post("/proxy/upstream/reset")
    cur = config_state()
    check("/admin/reset 后模式配置保持不变",
          cur["overrides"].get("900") == "personal"
          and cur["revision"] == rev1 + 1,
          f"rev={cur['revision']} overrides={cur['overrides']}")

    section("14. 丢回执 -> 待核实 -> 闸关闭 -> 核实收敛")
    proxy_post("/proxy/upstream/chaos", {"drop_receipt": True})
    rev2 = config_state()["revision"]
    s, d = approval(OPS_A, "POST", "/approval/api/proposals", {
        "notice_id": "903", "target_profile": "personal",
        "base_revision": rev2,
    })
    pid3 = d["proposal"]["proposal_id"]
    approval(OPS_B, "POST", f"/approval/api/proposals/{pid3}/approve")
    t0 = time.monotonic()
    s, d = approval(OPS_A, "POST", f"/approval/api/proposals/{pid3}/submit")
    submit_s = time.monotonic() - t0
    check("丢回执时提交返回 202 待核实（非成功/失败）",
          s == 202 and d["proposal"]["status"] == "UNKNOWN",
          f"{s} {d['proposal']['status']}")
    check("等待时长≈提交超时（回执被丢弃）", submit_s >= 4.0,
          f"{submit_s:.1f}s")
    # 上游实际已生效（配置与回执已持久化）
    cur = config_state()
    check("上游实际已生效且修订已前进",
          cur["revision"] == rev2 + 1
          and cur["overrides"].get("903") == "personal",
          f"rev={cur['revision']} overrides={cur['overrides']}")
    # 闸关闭：另一笔不得放行
    s, d2 = approval(OPS_A, "POST", "/approval/api/proposals", {
        "notice_id": "904", "target_profile": "personal",
        "base_revision": rev2 + 1,
    })
    pid4 = d2["proposal"]["proposal_id"]
    approval(OPS_B, "POST", f"/approval/api/proposals/{pid4}/approve")
    s, d2 = approval(OPS_A, "POST", f"/approval/api/proposals/{pid4}/submit")
    check("存在待核实提案时下一笔被拒绝（不放行）", s == 409, str(s))
    # 核实 -> 按上游回执收敛
    s, d = approval(OPS_B, "POST", f"/approval/api/proposals/{pid3}/verify")
    check("核实后收敛为已生效",
          s == 200 and d["resolved"] and d["proposal"]["status"] == "APPLIED",
          f"{s} {d.get('proposal', {}).get('status')}")
    check("核实到的回执即上游原回执",
          d["proposal"]["receipt"]["new_revision"] == rev2 + 1)
    # 闸已开，放行下一笔
    proxy_post("/proxy/upstream/chaos", {"drop_receipt": False})
    s, d2 = approval(OPS_A, "POST", f"/approval/api/proposals/{pid4}/submit")
    check("核实后下一笔可正常提交生效",
          d2["proposal"]["status"] == "APPLIED", d2["proposal"]["status"])

    section("15. 重试原提案：按提案 ID 去重，不重复生效")
    proxy_post("/proxy/upstream/chaos", {"drop_receipt": True})
    rev3 = config_state()["revision"]
    s, d = approval(OPS_A, "POST", "/approval/api/proposals", {
        "notice_id": "905", "target_profile": "personal",
        "base_revision": rev3,
    })
    pid5 = d["proposal"]["proposal_id"]
    approval(OPS_B, "POST", f"/approval/api/proposals/{pid5}/approve")
    s, d = approval(OPS_A, "POST", f"/approval/api/proposals/{pid5}/submit")
    check("首次提交丢回执 -> 待核实", d["proposal"]["status"] == "UNKNOWN",
          d["proposal"]["status"])
    check("上游已静默生效一次", config_state()["revision"] == rev3 + 1)
    proxy_post("/proxy/upstream/chaos", {"drop_receipt": False})
    s, d = approval(OPS_A, "POST", f"/approval/api/proposals/{pid5}/retry")
    check("重试后按原回执收敛为已生效",
          d["proposal"]["status"] == "APPLIED", d["proposal"]["status"])
    check("重试返回的是原回执（同一新修订）",
          d["proposal"]["receipt"]["new_revision"] == rev3 + 1,
          str(d["proposal"]["receipt"]))
    check("未重复生效（修订未再前进）",
          config_state()["revision"] == rev3 + 1,
          str(config_state()["revision"]))

    section("16. 网络等待期间不持数据库写锁")
    proxy_post("/proxy/upstream/chaos", {"drop_receipt": True})
    rev4 = config_state()["revision"]
    s, d = approval(OPS_A, "POST", "/approval/api/proposals", {
        "notice_id": "906", "target_profile": "personal",
        "base_revision": rev4,
    })
    pid6 = d["proposal"]["proposal_id"]
    approval(OPS_B, "POST", f"/approval/api/proposals/{pid6}/approve")
    submit_done: list = []

    def slow_submit():
        submit_done.append(
            approval(OPS_A, "POST",
                     f"/approval/api/proposals/{pid6}/submit", timeout=40))

    t_sub = threading.Thread(target=slow_submit)
    t_sub.start()
    time.sleep(1.0)  # 提交正在等待上游（回执被丢弃，约 5s 超时）
    t0 = time.monotonic()
    s, d = approval(OPS_A, "POST", "/approval/api/proposals", {
        "notice_id": "907", "target_profile": "personal",
        "base_revision": rev4 + 1,
    })
    create_ms = (time.monotonic() - t0) * 1000
    check("提交等待期间新提案可快速落库（不持写锁）",
          s == 201 and create_ms < 2000, f"{s}, {create_ms:.0f}ms")
    pid7 = d["proposal"]["proposal_id"]
    s, _ = approval(OPS_B, "POST", f"/approval/api/proposals/{pid7}/approve")
    check("提交等待期间批准亦可落库", s == 200, str(s))
    t_sub.join()
    check("阻塞中的提交最终判为待核实",
          submit_done[0][1]["proposal"]["status"] == "UNKNOWN",
          str(submit_done[0][1]["proposal"]["status"]))
    proxy_post("/proxy/upstream/chaos", {"drop_receipt": False})
    s, d = approval(OPS_A, "POST", f"/approval/api/proposals/{pid6}/verify")
    check("核实后闸释放", d["resolved"] is True
          and d["proposal"]["status"] == "APPLIED")

    section("17. 清理：测试公告恢复 public")
    for nid in ("900", "901", "903", "904", "905", "906"):
        ensure_profile(nid, "public")
    cur = config_state()
    check("测试公告均已恢复 public",
          all(cur["overrides"].get(n) == "public"
              for n in ("900", "901", "903", "904", "905", "906")),
          str(cur["overrides"]))
    st = approval_state()
    check("提交闸最终处于开放状态", st["gate"]["open"] is True)

    print(f"\n========== 结果：{PASS} 通过 / {FAIL} 失败 ==========")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
