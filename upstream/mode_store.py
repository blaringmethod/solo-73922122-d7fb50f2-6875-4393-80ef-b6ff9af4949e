"""上游受控配置（公告 public/personal 模式）与回执的 SQLite 持久化。

职责与不变量：
1. 模式配置只能经本模块的 apply() 变更（双人审批的应用入口）；演示旋钮
   （延迟/失败/max-age 等）不属于受控配置，不经过这里。
2. 每次成功应用使全局 revision 单调 +1；提案携带的 base_revision（发起人所见
   修订）与当前不一致即拒绝（旧修订拒绝），拒绝同样落成回执。
3. 按 proposal_id 去重：同一提案重复提交，返回首次持久化的回执，不重复生效。
4. 配置变更与回执在同一个 SQLite 事务中写入：要么都生效，要么都不生效。
5. 所有写操作都是短事务；本模块不做任何网络等待，调用方也不得在持有本模块
   锁期间进行网络等待。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from typing import Any

DEFAULT_SCOPE = "__default__"
PROFILES = ("public", "personal")

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS mode_config (
  scope   TEXT PRIMARY KEY,           -- '__default__' 或公告 ID
  profile TEXT NOT NULL CHECK (profile IN ('public','personal'))
);
CREATE TABLE IF NOT EXISTS receipts (
  proposal_id  TEXT PRIMARY KEY,      -- 提案 ID 去重键
  outcome      TEXT NOT NULL,         -- applied | rejected
  receipt_json TEXT NOT NULL,         -- 完整回执（含配置快照与修订）
  created_at   REAL NOT NULL
);
"""


class ModeStore:
    """线程安全的 SQLite 持久化存储（每个操作独立短事务）。"""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        parent = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(parent, exist_ok=True)
        # 进程内串行化写；跨进程一致性由 SQLite 事务（BEGIN IMMEDIATE）保证
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    def _init_db(self) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.executescript(SCHEMA)
                conn.execute(
                    "INSERT OR IGNORE INTO meta(key, value) VALUES('revision', '1')"
                )
                conn.execute(
                    "INSERT OR IGNORE INTO mode_config(scope, profile)"
                    " VALUES(?, 'public')",
                    (DEFAULT_SCOPE,),
                )
                conn.commit()
            finally:
                conn.close()

    # ---------------- 只读 ----------------

    def current(self) -> dict[str, Any]:
        """当前受控配置快照：{revision, default_profile, overrides}。"""
        with self._lock:
            conn = self._connect()
            try:
                revision = int(
                    conn.execute(
                        "SELECT value FROM meta WHERE key='revision'"
                    ).fetchone()["value"]
                )
                rows = conn.execute(
                    "SELECT scope, profile FROM mode_config ORDER BY scope"
                ).fetchall()
            finally:
                conn.close()
        default_profile = "public"
        overrides: dict[str, str] = {}
        for row in rows:
            if row["scope"] == DEFAULT_SCOPE:
                default_profile = row["profile"]
            else:
                overrides[row["scope"]] = row["profile"]
        return {
            "revision": revision,
            "default_profile": default_profile,
            "overrides": overrides,
        }

    def profile_for(self, notice_id: str) -> str:
        snap = self.current()
        return snap["overrides"].get(str(notice_id), snap["default_profile"])

    def get_receipt(self, proposal_id: str) -> dict[str, Any] | None:
        """按提案 ID 取已持久化的回执（核实入口）；无则 None。"""
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT receipt_json FROM receipts WHERE proposal_id = ?",
                    (proposal_id,),
                ).fetchone()
            finally:
                conn.close()
        return json.loads(row["receipt_json"]) if row else None

    # ---------------- 应用（唯一写入口） ----------------

    def apply(self, proposal: dict[str, Any]) -> tuple[dict[str, Any], int, bool]:
        """幂等应用提案。

        返回 (receipt, http_status, deduped)：
        - 已存在同 proposal_id 回执：原样返回，deduped=True，不重复生效；
        - base_revision 过期：写入拒绝回执（持久化），status=409；
        - 正常：同一事务内改配置 + revision+1 + 写回执，status=200。
        """
        proposal_id = str(proposal["proposal_id"])
        notice_id = str(proposal["notice_id"])
        target = str(proposal["target_profile"])
        if target not in PROFILES:
            raise ValueError(f"invalid target_profile: {target!r}")

        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    existing = conn.execute(
                        "SELECT receipt_json FROM receipts WHERE proposal_id = ?",
                        (proposal_id,),
                    ).fetchone()
                    if existing is not None:
                        # 同提案重试：返回原回执，不重复生效
                        conn.commit()
                        receipt = json.loads(existing["receipt_json"])
                        return receipt, 200 if receipt["outcome"] == "applied" else 409, True

                    current_rev = int(
                        conn.execute(
                            "SELECT value FROM meta WHERE key='revision'"
                        ).fetchone()["value"]
                    )
                    now = time.time()
                    receipt: dict[str, Any] = {
                        "proposal_id": proposal_id,
                        "notice_id": notice_id,
                        "target_profile": target,
                        "base_revision": int(proposal["base_revision"]),
                        "proposer": proposal.get("proposer"),
                        "approver": proposal.get("approver"),
                        "processed_at": now,
                    }
                    if int(proposal["base_revision"]) != current_rev:
                        # 旧修订拒绝：拒绝结果同样作为回执持久化
                        receipt.update(
                            outcome="rejected",
                            reason="stale_revision",
                            detail=(
                                f"提案基于修订 {proposal['base_revision']}，"
                                f"当前修订 {current_rev}"
                            ),
                            current_revision=current_rev,
                            new_revision=None,
                        )
                        conn.execute(
                            "INSERT INTO receipts"
                            "(proposal_id, outcome, receipt_json, created_at)"
                            " VALUES(?, 'rejected', ?, ?)",
                            (proposal_id, json.dumps(receipt), now),
                        )
                        conn.commit()
                        return receipt, 409, False

                    new_rev = current_rev + 1
                    # 配置与回执同事务提交：要么都生效，要么都不生效
                    conn.execute(
                        "INSERT INTO mode_config(scope, profile) VALUES(?, ?)"
                        " ON CONFLICT(scope) DO UPDATE SET profile=excluded.profile",
                        (notice_id, target),
                    )
                    conn.execute(
                        "UPDATE meta SET value = ? WHERE key = 'revision'",
                        (str(new_rev),),
                    )
                    receipt.update(
                        outcome="applied",
                        reason=None,
                        detail=None,
                        current_revision=current_rev,
                        new_revision=new_rev,
                    )
                    conn.execute(
                        "INSERT INTO receipts"
                        "(proposal_id, outcome, receipt_json, created_at)"
                        " VALUES(?, 'applied', ?, ?)",
                        (proposal_id, json.dumps(receipt), now),
                    )
                    conn.commit()
                    return receipt, 200, False
                except Exception:
                    conn.rollback()
                    raise
            finally:
                conn.close()
