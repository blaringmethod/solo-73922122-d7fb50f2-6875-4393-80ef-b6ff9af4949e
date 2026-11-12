"""本地演示上游（固定上游的模拟实现）。

能力：
- 公告内容按 Accept-Language 返回中/英双语；
- 公告 public / personal 模式为受控配置：只经双人审批应用入口
  （POST /admin/apply）变更，SQLite 持久化，按提案 ID 幂等；
  /admin/config 与 /admin/reset 一律不得改动模式（不得绕过审批）；
- 可配置人工延迟与失败模式（500 / 超长延迟触发代理超时）——演示旋钮，
  不属于受控配置；
- 混沌开关：生效后丢回执（drop_receipt），用于演练代理"待核实"路径；
- 管理端可查看请求计数、最近请求记录、受控配置修订与回执。
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from collections import deque
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .mode_store import ModeStore

UPSTREAM_DB = os.environ.get("UPSTREAM_DB", ".run/upstream.db")
# 丢回执时上游沉默的时长（须大于代理提交超时，代理才会判"待核实"）
DROP_RECEIPT_SECONDS = float(os.environ.get("DROP_RECEIPT_SECONDS", "30"))

app = FastAPI(title="Mock Upstream", version="2.0.0")
mode_store = ModeStore(UPSTREAM_DB)

NOTICES: dict[str, dict[str, dict[str, str]]] = {
    "1": {
        "zh": {
            "title": "关于年度系统维护的通知",
            "body": "本周六 02:00-04:00 门户系统例行维护，期间暂停服务。",
        },
        "en": {
            "title": "Annual System Maintenance Notice",
            "body": "Routine maintenance this Saturday 02:00-04:00; service paused.",
        },
    },
    "2": {
        "zh": {
            "title": "新版内网门户上线公告",
            "body": "新版门户已上线，支持统一搜索与个性化工作台。",
        },
        "en": {
            "title": "New Intranet Portal Launch",
            "body": "The new portal is live with unified search and a personal dashboard.",
        },
    },
    "3": {
        "zh": {
            "title": "信息安全培训报名通知",
            "body": "请各部门员工于月底前完成信息安全培训报名。",
        },
        "en": {
            "title": "Security Training Enrollment",
            "body": "All staff should enroll in the security training before month-end.",
        },
    },
}

# 演示旋钮（非受控配置）：不含 profile，profile 只经审批应用入口变更
DEFAULT_CONFIG = {
    "latency_ms": 200,
    "failure": "none",          # none | error500
    "max_age": 30,
    "cache_control": None,      # 非空时作为原始 Cache-Control 直接下发
}

state: dict[str, Any] = {
    "defaults": dict(DEFAULT_CONFIG),
    "overrides": {},
    "content_version": 1,
    "started_at": time.time(),
}
# 混沌开关（演示用，内存态）：生效后丢回执
chaos: dict[str, Any] = {"drop_receipt": False}
counters = {"total_requests": 0, "by_notice": {}, "by_profile": {}}
request_log: deque[dict[str, Any]] = deque(maxlen=50)
_log_lock = asyncio.Lock()


def _cfg_for(notice_id: str) -> dict[str, Any]:
    cfg = dict(state["defaults"])
    cfg.update(state["overrides"].get(notice_id, {}))
    return cfg


def _pick_language(accept_language: str) -> str:
    if not accept_language:
        return "zh"
    for part in accept_language.split(","):
        tag = part.split(";", 1)[0].strip().lower()
        if tag.startswith("zh"):
            return "zh"
        if tag.startswith("en"):
            return "en"
    return "zh"


def _bump_version() -> None:
    state["content_version"] += 1


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/notices/{notice_id}")
async def get_notice(notice_id: str, request: Request) -> JSONResponse:
    cfg = _cfg_for(notice_id)
    # 受控配置（public/personal）来自 SQLite 权威存储
    profile = await asyncio.to_thread(mode_store.profile_for, notice_id)
    request_id = uuid.uuid4().hex[:12]
    started = time.perf_counter()

    if cfg["latency_ms"]:
        await asyncio.sleep(cfg["latency_ms"] / 1000.0)

    cookie = request.headers.get("cookie", "")
    auth = request.headers.get("authorization", "")
    lang = _pick_language(request.headers.get("accept-language", ""))

    log_entry = {
        "request_id": request_id,
        "notice_id": notice_id,
        "language": lang,
        "accept_language_raw": request.headers.get("accept-language", ""),
        "has_cookie": bool(cookie.strip()),
        "has_authorization": bool(auth.strip()),
        "profile": profile,
        "failure": cfg["failure"],
        "latency_ms": cfg["latency_ms"],
        "elapsed_ms": round((time.perf_counter() - started) * 1000, 1),
        "ts": round(time.time(), 3),
    }
    counters["total_requests"] += 1
    counters["by_notice"][notice_id] = counters["by_notice"].get(notice_id, 0) + 1
    counters["by_profile"][profile] = counters["by_profile"].get(profile, 0) + 1
    async with _log_lock:
        request_log.appendleft(log_entry)

    base_headers = {
        "X-Upstream-Request-Id": request_id,
        "X-Notice-Id": notice_id,
        "X-Notice-Kind": profile,
    }

    if cfg["failure"] == "error500":
        return JSONResponse(
            status_code=500,
            content={"error": "simulated upstream failure", "request_id": request_id},
            headers=base_headers,
        )

    if notice_id not in NOTICES:
        return JSONResponse(
            status_code=404,
            content={"error": f"notice {notice_id} not found", "request_id": request_id},
            headers=base_headers,
        )

    content = NOTICES[notice_id][lang]
    payload = {
        "notice_id": notice_id,
        "language": lang,
        "title": content["title"],
        "body": content["body"],
        "profile": profile,
        "content_version": state["content_version"],
        "served_by": "mock-upstream",
        "request_id": request_id,
        "identity_seen": {
            "cookie": cookie or None,
            "authorization": auth or None,
        },
    }

    headers = dict(base_headers)
    headers["Content-Language"] = lang
    if cfg["cache_control"] is not None:
        headers["Cache-Control"] = str(cfg["cache_control"])
    elif profile == "public":
        headers["Cache-Control"] = f"public, max-age={int(cfg['max_age'])}"
    else:
        # 个人页面：显式私有、禁止缓存，并下发 Set-Cookie
        headers["Cache-Control"] = "private, no-store"
        headers["Set-Cookie"] = (
            f"SESSION=user-{request_id}; Path=/; HttpOnly; SameSite=Lax"
        )

    return JSONResponse(status_code=200, content=payload, headers=headers)


# ---------------- 管理接口 ----------------


@app.get("/admin/state")
async def admin_state() -> dict[str, Any]:
    return {
        "defaults": state["defaults"],
        "overrides": state["overrides"],
        "content_version": state["content_version"],
        "counters": counters,
        "mode": await asyncio.to_thread(mode_store.current),
        "chaos": {**chaos, "drop_seconds": DROP_RECEIPT_SECONDS},
        "uptime_seconds": round(time.time() - state["started_at"], 1),
    }


@app.get("/admin/requests")
async def admin_requests() -> dict[str, Any]:
    async with _log_lock:
        return {"requests": list(request_log)}


@app.post("/admin/config")
async def admin_config(payload: dict[str, Any]) -> JSONResponse:
    """演示旋钮配置（延迟/失败/max-age/自定义 Cache-Control）。

    受控的 public/personal 模式不在此列：一律拒绝，必须走双人审批
    （POST /admin/apply），本入口不得绕过审批。
    """
    if "profile" in payload:
        return JSONResponse(
            status_code=400,
            content={
                "error": "profile 为受控配置，本入口不得绕过双人审批；"
                         "请使用审批页提案（代理 /approval）"
            },
        )
    notice_id = payload.get("notice_id")
    fields = {
        k: payload[k]
        for k in ("latency_ms", "failure", "max_age", "cache_control")
        if k in payload
    }
    if "failure" in fields and fields["failure"] not in ("none", "error500"):
        return JSONResponse(
            status_code=400, content={"error": "failure must be none|error500"}
        )
    if notice_id:
        target = state["overrides"].setdefault(str(notice_id), {})
        target.update(fields)
    else:
        state["defaults"].update(fields)
    _bump_version()
    return JSONResponse({"ok": True, "state": await admin_state()})


@app.post("/admin/reset")
async def admin_reset() -> JSONResponse:
    """重置演示旋钮与统计；受控模式配置（profile）保持不变，不得绕过审批。"""
    state["defaults"] = dict(DEFAULT_CONFIG)
    state["overrides"] = {}
    _bump_version()
    counters["total_requests"] = 0
    counters["by_notice"] = {}
    counters["by_profile"] = {}
    async with _log_lock:
        request_log.clear()
    return JSONResponse(
        {
            "ok": True,
            "note": "演示旋钮与统计已重置；public/personal 模式配置保持不变"
            "（受双人审批管控）",
            "state": await admin_state(),
        }
    )


# ---------------- 受控配置：双人审批应用入口 ----------------


def _validate_proposal(payload: dict[str, Any]) -> str | None:
    for field in ("proposal_id", "notice_id", "target_profile",
                  "base_revision", "proposer", "approver"):
        if payload.get(field) in (None, ""):
            return f"missing field: {field}"
    if payload["target_profile"] not in ("public", "personal"):
        return "target_profile must be public|personal"
    try:
        if int(payload["base_revision"]) < 1:
            return "base_revision must be >= 1"
    except (TypeError, ValueError):
        return "base_revision must be an integer"
    if payload["proposer"] == payload["approver"]:
        return "proposer 与 approver 不得为同一人"
    return None


@app.post("/admin/apply")
async def admin_apply(payload: dict[str, Any]) -> JSONResponse:
    """应用已批准提案（唯一允许变更 public/personal 模式的入口）。

    - 按 proposal_id 去重：同提案重试返回原回执，不重复生效；
    - 核对所见修订：base_revision 过期即拒绝，拒绝同样落成回执；
    - 配置与回执在同一 SQLite 事务持久化；
    - 混沌开关 drop_receipt 开启时：先生效并持久化，再沉默不回传回执
      （模拟生效后丢回执/断流，代理应判"待核实"）。
    """
    error = _validate_proposal(payload)
    if error:
        return JSONResponse(status_code=400, content={"error": error})
    receipt, status, deduped = await asyncio.to_thread(mode_store.apply, payload)
    if chaos["drop_receipt"]:
        # 生效后丢回执：配置与回执已持久化，但响应迟迟不回，迫使代理超时
        await asyncio.sleep(DROP_RECEIPT_SECONDS)
    return JSONResponse(
        status_code=status,
        content={"receipt": receipt, "deduped": deduped},
        headers={"X-Receipt-Deduped": "true" if deduped else "false"},
    )


@app.get("/admin/receipts/{proposal_id}")
async def admin_receipt(proposal_id: str) -> JSONResponse:
    """按提案 ID 查询已持久化回执（代理核实入口）。"""
    receipt = await asyncio.to_thread(mode_store.get_receipt, proposal_id)
    if receipt is None:
        return JSONResponse(
            status_code=404,
            content={"error": "no receipt for proposal", "proposal_id": proposal_id},
        )
    return JSONResponse({"receipt": receipt})


@app.get("/admin/config-state")
async def admin_config_state() -> dict[str, Any]:
    """受控配置实况（修订、默认模式、按公告覆盖）与混沌开关状态。"""
    snap = await asyncio.to_thread(mode_store.current)
    return {**snap, "chaos": {**chaos, "drop_seconds": DROP_RECEIPT_SECONDS}}


@app.post("/admin/chaos")
async def admin_chaos(payload: dict[str, Any]) -> dict[str, Any]:
    """混沌开关：drop_receipt=true 时，/admin/apply 生效后丢回执。"""
    if "drop_receipt" in payload:
        chaos["drop_receipt"] = bool(payload["drop_receipt"])
    return {"ok": True, "chaos": {**chaos, "drop_seconds": DROP_RECEIPT_SECONDS}}
