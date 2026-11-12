"""FastAPI 单进程 HTTP 缓存代理。

规则摘要：
- 仅代理 GET /notices/{notice_id}，上游地址固定（环境变量 UPSTREAM_BASE）。
- 带 Cookie 或 Authorization 的请求直连上游：不读、不写缓存，也不参与
  匿名请求合并。
- 匿名响应仅在 200 + Cache-Control 含 public 且含合法非负整数 max-age、
  无 private/no-store、无 Set-Cookie 时缓存；过期不复用。
- 同键并发未命中只请求上游一次；单个等待者断开不影响其他等待者；
  上游失败不缓存并结束所有等待。
- 按公告清除会删除该公告全部语言缓存，并使清除前的在途响应无法回填、
  清除后的新请求无法复用旧在途结果。
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hmac
import os
import time
from dataclasses import dataclass
from pathlib import Path

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse

from .approval_store import (
    UNKNOWN,
    ApprovalError,
    ApprovalStore,
)
from .cachestore import (
    CachedResponse,
    CacheHit,
    CacheLeader,
    CacheStore,
    CacheWait,
    InFlight,
)
from .policy import cache_eligibility

UPSTREAM_BASE = os.environ.get("UPSTREAM_BASE", "http://upstream:8001").rstrip("/")
HTTP_TIMEOUT = float(os.environ.get("UPSTREAM_TIMEOUT_SECONDS", "15"))
# 审批提案提交上游的超时：超时/断流一律判"待核实"，不得视为成功或失败
SUBMIT_TIMEOUT = float(os.environ.get("APPROVAL_SUBMIT_TIMEOUT_SECONDS", "5"))
APPROVAL_DB_PATH = os.environ.get("PROXY_APPROVAL_DB", ".run/proxy-approval.db")


def _parse_ops_accounts(raw: str) -> dict[str, str]:
    """解析启动配置固定的运维账号：'user1:pass1,user2:pass2'，必须恰好两个。"""
    accounts: dict[str, str] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        user, sep, password = item.partition(":")
        if not sep or not user or not password:
            raise RuntimeError(f"OPS_ACCOUNTS 条目非法（须为 user:password）: {item!r}")
        if user in accounts:
            raise RuntimeError(f"OPS_ACCOUNTS 账号重复: {user!r}")
        accounts[user] = password
    if len(accounts) != 2:
        raise RuntimeError("OPS_ACCOUNTS 必须恰好配置两个运维账号（双人复核）")
    return accounts


OPS_ACCOUNTS = _parse_ops_accounts(
    os.environ.get("OPS_ACCOUNTS", "alice:alice-pass,bob:bob-pass")
)

# 转发给上游的请求头白名单（逐跳头、Host 等不带）
FORWARD_REQUEST_HEADERS = {
    "accept",
    "accept-language",
    "user-agent",
    "cookie",
    "authorization",
    "x-request-id",
}

# 回传给客户端 / 写入缓存的响应头白名单。
# content-length / transfer-encoding / content-encoding 由框架按解码后的
# body 重新处理；Set-Cookie 仅直连（带身份）响应可能出现。
PASS_RESPONSE_HEADERS = {
    "content-type",
    "content-language",
    "cache-control",
    "etag",
    "last-modified",
    "set-cookie",
    "x-upstream-request-id",
    "x-notice-id",
    "x-notice-kind",
}

HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
    "content-encoding",
}

STATIC_DIR = Path(__file__).resolve().parent / "static"

app = FastAPI(title="Notice Cache Proxy", version="2.0.0")
store = CacheStore()
approval_store = ApprovalStore(APPROVAL_DB_PATH)
_client: httpx.AsyncClient | None = None


@dataclass
class FetchResult:
    status_code: int
    body: bytes
    headers: list[tuple[str, str]]
    request_id: str | None


@app.on_event("startup")
async def _startup() -> None:
    global _client
    _client = httpx.AsyncClient(
        base_url=UPSTREAM_BASE,
        timeout=httpx.Timeout(HTTP_TIMEOUT),
        follow_redirects=False,
    )
    # 崩溃恢复：重启时凡是"提交中"的提案结果不可知，一律转为"待核实"，
    # 由运维核实或重试原提案（上游按提案 ID 幂等，重试安全）。
    recovered = await asyncio.to_thread(approval_store.recover_after_restart)
    if recovered:
        print(f"[approval] 重启恢复：{len(recovered)} 笔提交中提案转为待核实: "
              f"{[p[:8] for p in recovered]}")


@app.on_event("shutdown")
async def _shutdown() -> None:
    if _client is not None:
        await _client.aclose()


def _is_personal(request: Request) -> bool:
    cookie = request.headers.get("cookie", "").strip()
    auth = request.headers.get("authorization", "").strip()
    return bool(cookie) or bool(auth)


def _forward_headers(request: Request) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in request.headers.items():
        lk = k.lower()
        if lk in FORWARD_REQUEST_HEADERS and lk not in HOP_BY_HOP:
            out[k] = v
    return out


def _filter_response_headers(
    headers, *, include_set_cookie: bool
) -> list[tuple[str, str]]:
    result: list[tuple[str, str]] = []
    for k, v in headers:
        lk = k.lower()
        if lk in HOP_BY_HOP:
            continue
        if lk == "set-cookie" and not include_set_cookie:
            continue
        if lk in PASS_RESPONSE_HEADERS:
            result.append((k, v))
    return result


async def _fetch_upstream(request: Request, notice_id: str) -> FetchResult:
    assert _client is not None
    resp = await _client.get(
        f"/notices/{notice_id}", headers=_forward_headers(request)
    )
    body = resp.content  # 读取完整 body，连接即可释放
    return FetchResult(
        status_code=resp.status_code,
        body=body,
        headers=list(resp.headers.multi_items()),
        request_id=resp.headers.get("x-upstream-request-id"),
    )


def _build_cached_entry(
    result: FetchResult, generation: int
) -> CachedResponse | None:
    ok, _reason, max_age = cache_eligibility(result.status_code, result.headers)
    if not ok or max_age is None:
        return None
    safe_headers = [
        (k.encode("latin-1"), v.encode("latin-1"))
        for k, v in _filter_response_headers(
            result.headers, include_set_cookie=False
        )
    ]
    now = time.monotonic()
    return CachedResponse(
        status_code=result.status_code,
        body=result.body,
        headers=safe_headers,
        cached_at=now,
        expires_at=now + max_age,
        max_age=max_age,
        generation=generation,
    )


class UpstreamFailure(Exception):
    """上游返回 5xx：视为失败，不缓存并结束所有等待者。"""

    def __init__(self, result: "FetchResult") -> None:
        super().__init__(f"upstream responded {result.status_code}")
        self.result = result


async def _leader_work(
    inflight: InFlight, request: Request, notice_id: str
) -> None:
    """独立任务执行上游请求；任何等待者（含 leader 客户端）断开都不会取消它。

    成功且可缓存：仅在仍是当前代际时回填，再交付全部等待者。
    传输异常 / 上游 5xx：不缓存，以异常结束所有等待者。
    """
    try:
        result = await _fetch_upstream(request, notice_id)
        if result.status_code >= 500:
            raise UpstreamFailure(result)
    except BaseException as exc:  # noqa: BLE001 - 需传播给全部等待者
        await store.fail_all(inflight, exc)
        if not inflight.future.done():
            inflight.future.set_exception(exc)
        return
    try:
        entry = _build_cached_entry(result, inflight.generation)
        await store.store_if_current(inflight, entry)
    finally:
        if not inflight.future.done():
            inflight.future.set_result(result)


def _live_response(
    result: FetchResult, *, cache_status: str, reason: str
) -> Response:
    # raw_headers 支持重复的 Set-Cookie；与缓存条目一样使用 latin-1 字节
    raw = [
        (k.encode("latin-1"), v.encode("latin-1"))
        for k, v in _filter_response_headers(
            result.headers, include_set_cookie=True
        )
    ]
    raw.append((b"x-cache", cache_status.encode("latin-1")))
    raw.append((b"x-cache-reason", reason.encode("latin-1")))
    response = Response(content=result.body, status_code=result.status_code)
    response.raw_headers = raw
    return response


def _error_response(message: str, status: int = 502) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={"detail": message, "upstream": UPSTREAM_BASE},
        headers={"X-Cache": "ERROR"},
    )


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/notices/{notice_id}")
async def proxy_notice(notice_id: str, request: Request) -> Response:
    # ---- 带身份的请求：直连，不碰缓存、不参与合并 ----
    if _is_personal(request):
        try:
            result = await _fetch_upstream(request, notice_id)
        except httpx.HTTPError as exc:
            return _error_response(f"upstream error (bypass): {exc}")
        if result.status_code >= 500:
            return _error_response(
                f"upstream responded {result.status_code} (bypass)"
            )
        return _live_response(
            result, cache_status="BYPASS", reason="authenticated"
        )

    accept_language = request.headers.get("accept-language", "")
    key = store.make_key(notice_id, accept_language)
    decision = await store.lookup(key, notice_id)

    # ---- 1) 缓存命中（过期条目在 lookup 阶段已被剔除，绝不复用）----
    if isinstance(decision, CacheHit):
        entry = decision.entry
        raw = list(entry.headers)
        age = int(time.monotonic() - entry.cached_at)
        raw.append((b"x-cache", b"HIT"))
        raw.append((
            b"x-cache-key",
            f"notice={notice_id};lang={accept_language or '<none>'}".encode("latin-1"),
        ))
        raw.append((b"age", str(age).encode("latin-1")))
        response = Response(content=entry.body, status_code=entry.status_code)
        response.raw_headers = raw
        return response

    # ---- 2) 已有同键在途：加入等待，不发新的上游请求 ----
    if isinstance(decision, CacheWait):
        try:
            result = await decision.inflight.future
        except UpstreamFailure as exc:
            return _error_response(
                f"upstream responded {exc.result.status_code} (shared)"
            )
        except httpx.HTTPError as exc:
            return _error_response(f"upstream error (shared): {exc}")
        except Exception as exc:  # noqa: BLE001
            return _error_response(f"shared request failed: {exc}")
        return _live_response(
            result,
            cache_status="MISS-WAIT",
            reason="coalesced with in-flight request",
        )

    # ---- 3) leader：发起唯一的上游请求 ----
    # 独立任务 + 等待 future：leader 客户端断开只会取消路由处理任务，
    # 不会取消 leader_work，其他等待者照常得到结果。
    assert isinstance(decision, CacheLeader)
    inflight = decision.inflight
    task = asyncio.create_task(_leader_work(inflight, request, notice_id))
    inflight.leader_task = task
    try:
        result = await inflight.future
    except UpstreamFailure as exc:
        return _error_response(
            f"upstream responded {exc.result.status_code}"
        )
    except httpx.HTTPError as exc:
        return _error_response(f"upstream error: {exc}")
    except Exception as exc:  # noqa: BLE001
        return _error_response(f"upstream request failed: {exc}")

    ok, reason, _ = cache_eligibility(result.status_code, result.headers)
    return _live_response(
        result,
        cache_status="MISS" if ok else "MISS-NO-CACHE",
        reason=reason,
    )


# ---------------- 管理 / 演示接口 ----------------


def _passthrough(r: httpx.Response) -> Response:
    return Response(
        content=r.content,
        status_code=r.status_code,
        media_type=r.headers.get("content-type"),
    )


@app.get("/proxy/state")
async def proxy_state() -> JSONResponse:
    return JSONResponse(await store.snapshot())


@app.post("/proxy/purge/{notice_id}")
async def purge_notice(notice_id: str) -> JSONResponse:
    info = await store.purge_notice(notice_id)
    return JSONResponse({"purged": notice_id, **info})


@app.post("/proxy/reset")
async def proxy_reset() -> JSONResponse:
    return JSONResponse(await store.reset())


@app.get("/proxy/upstream/state")
async def upstream_state() -> Response:
    assert _client is not None
    try:
        return _passthrough(await _client.get("/admin/state"))
    except httpx.HTTPError as exc:
        return _error_response(f"upstream admin unreachable: {exc}")


@app.get("/proxy/upstream/requests")
async def upstream_requests() -> Response:
    assert _client is not None
    try:
        return _passthrough(await _client.get("/admin/requests"))
    except httpx.HTTPError as exc:
        return _error_response(f"upstream admin unreachable: {exc}")


@app.post("/proxy/upstream/config")
async def upstream_config(request: Request) -> Response:
    assert _client is not None
    payload = await request.body()
    try:
        r = await _client.post(
            "/admin/config",
            content=payload,
            headers={
                "content-type": request.headers.get(
                    "content-type", "application/json"
                )
            },
        )
    except httpx.HTTPError as exc:
        return _error_response(f"upstream admin unreachable: {exc}")
    return _passthrough(r)


@app.post("/proxy/upstream/reset")
async def upstream_reset() -> Response:
    assert _client is not None
    try:
        return _passthrough(await _client.post("/admin/reset"))
    except httpx.HTTPError as exc:
        return _error_response(f"upstream admin unreachable: {exc}")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


# ---------------- 双人审批：公告 public/personal 模式切换 ----------------
#
# 规则：
# - 两个运维账号由启动配置 OPS_ACCOUNTS 固定，HTTP Basic 认证；
# - 提案含公告 ID、目标模式、所见配置修订；创建后不可修改；发起人不能批准；
# - 代理一次只提交一笔；先把批准与提交状态落库，再请求上游；
# - 超时/断流 -> 待核实（UNKNOWN）：不报成功或失败，且不放行下一笔；
# - 核实以上游持久化回执为准；重试沿用原提案 ID，上游幂等不重复生效。


def _ops_user(request: Request) -> str:
    """HTTP Basic 认证：仅接受启动配置固定的两个运维账号。"""
    auth = request.headers.get("authorization", "")
    if auth.startswith("Basic "):
        user = password = ""
        try:
            decoded = base64.b64decode(auth[6:].strip(), validate=True).decode("utf-8")
            user, _, password = decoded.partition(":")
        except (binascii.Error, UnicodeDecodeError, ValueError):
            pass
        expected = OPS_ACCOUNTS.get(user)
        if expected is not None and hmac.compare_digest(password, expected):
            return user
    raise HTTPException(
        status_code=401,
        detail="需要运维账号（HTTP Basic）",
        headers={"WWW-Authenticate": 'Basic realm="ops-approval"'},
    )


OpsUser = Depends(_ops_user)


async def _upstream_config_state() -> dict:
    """尽力获取上游受控配置实况；不可达时返回标记，不阻断页面。"""
    assert _client is not None
    try:
        resp = await _client.get("/admin/config-state", timeout=3.0)
        return {"reachable": True, "config": resp.json()}
    except Exception as exc:  # noqa: BLE001 - 展示用，任何失败都降级
        return {"reachable": False, "error": f"{type(exc).__name__}: {exc}"}


@app.get("/approval")
async def approval_page(user: str = OpsUser) -> FileResponse:
    return FileResponse(STATIC_DIR / "approval.html")


@app.get("/approval/api/state")
async def approval_state(user: str = OpsUser) -> JSONResponse:
    proposals = await asyncio.to_thread(approval_store.list_all)
    blocker = await asyncio.to_thread(approval_store.blocking_proposal)
    upstream = await _upstream_config_state()
    return JSONResponse(
        {
            "me": user,
            "accounts": sorted(OPS_ACCOUNTS),
            "submit_timeout_seconds": SUBMIT_TIMEOUT,
            "gate": {"open": blocker is None, "blocking": blocker},
            "proposals": proposals,
            "upstream": upstream,
        }
    )


@app.post("/approval/api/proposals")
async def approval_create(request: Request, user: str = OpsUser) -> JSONResponse:
    """发起提案：公告 ID + 目标模式 + 所见配置修订。创建后不可修改。"""
    try:
        payload = await request.json()
    except Exception:  # noqa: BLE001
        return JSONResponse(status_code=400, content={"error": "请求体须为 JSON"})
    try:
        base_revision = int(payload.get("base_revision", 0))
    except (TypeError, ValueError):
        return JSONResponse(status_code=400, content={"error": "base_revision 须为整数"})
    try:
        proposal = await asyncio.to_thread(
            approval_store.create,
            notice_id=str(payload.get("notice_id", "")).strip(),
            target_profile=str(payload.get("target_profile", "")).strip(),
            base_revision=base_revision,
            proposer=user,
        )
    except ApprovalError as exc:
        return JSONResponse(status_code=exc.status_code, content={"error": str(exc)})
    return JSONResponse(status_code=201, content={"proposal": proposal})


@app.post("/approval/api/proposals/{proposal_id}/approve")
async def approval_approve(proposal_id: str, user: str = OpsUser) -> JSONResponse:
    """批准：仅另一账号可批；原子迁移，并发批准只生效一次。"""
    try:
        proposal = await asyncio.to_thread(
            approval_store.approve, proposal_id, user
        )
    except ApprovalError as exc:
        return JSONResponse(status_code=exc.status_code, content={"error": str(exc)})
    return JSONResponse({"proposal": proposal})


async def _submit_to_upstream(proposal_id: str) -> JSONResponse:
    """提交（或重试）一笔提案到上游。

    顺序保证：先把提交状态落库（同事务占用提交闸），再发起网络请求；
    网络等待期间不持有任何数据库连接/写锁。
    """
    try:
        proposal = await asyncio.to_thread(
            approval_store.begin_submit, proposal_id
        )
    except ApprovalError as exc:
        return JSONResponse(status_code=exc.status_code, content={"error": str(exc)})

    payload = {
        "proposal_id": proposal["proposal_id"],
        "notice_id": proposal["notice_id"],
        "target_profile": proposal["target_profile"],
        "base_revision": proposal["base_revision"],
        "proposer": proposal["proposer"],
        "approver": proposal["approver"],
    }
    assert _client is not None
    try:
        resp = await _client.post(
            "/admin/apply", json=payload, timeout=SUBMIT_TIMEOUT
        )
    except httpx.HTTPError as exc:
        # 超时/断流：结果不可知 —— 待核实，绝不报成功或失败
        updated = await asyncio.to_thread(
            approval_store.mark_unknown,
            proposal_id,
            f"{type(exc).__name__}: {exc}",
        )
        return JSONResponse(
            status_code=202,
            content={
                "proposal": updated,
                "message": "上游未在时限内返回回执（超时/断流）。结果待核实："
                "不得视为成功或失败；提交闸已关闭，须核实后才能放行下一笔。",
            },
        )

    if resp.status_code == 200:
        receipt = resp.json().get("receipt", {})
        updated = await asyncio.to_thread(
            approval_store.finish_applied, proposal_id, receipt
        )
        return JSONResponse(
            {"proposal": updated, "message": "上游回执：已生效。"}
        )
    if resp.status_code == 409:
        receipt = resp.json().get("receipt", {})
        updated = await asyncio.to_thread(
            approval_store.finish_rejected, proposal_id, receipt
        )
        return JSONResponse(
            {"proposal": updated, "message": "上游回执：已拒绝（如旧修订）。"}
        )
    # 非预期状态码：不可判定，按待核实处理
    updated = await asyncio.to_thread(
        approval_store.mark_unknown,
        proposal_id,
        f"unexpected upstream status {resp.status_code}: {resp.text[:200]}",
    )
    return JSONResponse(
        status_code=202,
        content={"proposal": updated, "message": "上游响应非预期，结果待核实。"},
    )


@app.post("/approval/api/proposals/{proposal_id}/submit")
async def approval_submit(proposal_id: str, user: str = OpsUser) -> JSONResponse:
    return await _submit_to_upstream(proposal_id)


@app.post("/approval/api/proposals/{proposal_id}/retry")
async def approval_retry(proposal_id: str, user: str = OpsUser) -> JSONResponse:
    """重试原提案（同一提案 ID，内容不可变）；上游按 ID 去重，不重复生效。"""
    return await _submit_to_upstream(proposal_id)


@app.post("/approval/api/proposals/{proposal_id}/verify")
async def approval_verify(proposal_id: str, user: str = OpsUser) -> JSONResponse:
    """核实待核实提案：以上游持久化的回执为准收敛本地状态。"""
    proposal = await asyncio.to_thread(approval_store.get, proposal_id)
    if proposal is None:
        return JSONResponse(status_code=404, content={"error": "提案不存在"})
    if proposal["status"] != UNKNOWN:
        return JSONResponse(
            status_code=409,
            content={
                "error": f"当前状态为 {proposal['status']}，仅待核实"
                "（UNKNOWN）状态需要核实"
            },
        )
    assert _client is not None
    try:
        resp = await _client.get(
            f"/admin/receipts/{proposal_id}", timeout=SUBMIT_TIMEOUT
        )
    except httpx.HTTPError as exc:
        return JSONResponse(
            status_code=202,
            content={
                "proposal": proposal,
                "resolved": False,
                "message": f"上游不可达，保持待核实：{type(exc).__name__}: {exc}",
            },
        )
    if resp.status_code == 404:
        return JSONResponse(
            {
                "proposal": proposal,
                "resolved": False,
                "message": "上游无此提案回执：请求可能从未到达，可安全重试原提案。",
            }
        )
    if resp.status_code != 200:
        return JSONResponse(
            status_code=202,
            content={
                "proposal": proposal,
                "resolved": False,
                "message": f"上游核实接口返回 {resp.status_code}，保持待核实。",
            },
        )
    receipt = resp.json()["receipt"]
    try:
        updated = await asyncio.to_thread(
            approval_store.resolve_from_receipt, proposal_id, receipt
        )
    except ApprovalError as exc:
        return JSONResponse(status_code=exc.status_code, content={"error": str(exc)})
    return JSONResponse(
        {"proposal": updated, "resolved": True, "message": "已按上游回执核实。"}
    )


@app.get("/proxy/upstream/config-state")
async def upstream_config_state() -> Response:
    assert _client is not None
    try:
        return _passthrough(await _client.get("/admin/config-state"))
    except httpx.HTTPError as exc:
        return _error_response(f"upstream admin unreachable: {exc}")


@app.post("/proxy/upstream/chaos")
async def upstream_chaos(request: Request) -> Response:
    assert _client is not None
    payload = await request.body()
    try:
        r = await _client.post(
            "/admin/chaos",
            content=payload,
            headers={
                "content-type": request.headers.get(
                    "content-type", "application/json"
                )
            },
        )
    except httpx.HTTPError as exc:
        return _error_response(f"upstream admin unreachable: {exc}")
    return _passthrough(r)
