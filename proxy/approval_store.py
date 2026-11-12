"""代理侧双人审批提案的 SQLite 持久化状态机。

状态机：
    PENDING --approve(非发起人)--> APPROVED --begin_submit--> SUBMITTING
    SUBMITTING --上游明确回执--> APPLIED / REJECTED（终态）
    SUBMITTING --超时/断流--> UNKNOWN（待核实：既不算成功也不算失败）
    UNKNOWN --核实到上游回执--> APPLIED / REJECTED
    UNKNOWN --重试原提案（同 ID）--> SUBMITTING

不变量：
1. 提案内容（公告/目标模式/所见修订/发起人）创建后不可修改——本模块不提供
   任何修改这些列的方法，只有状态迁移。
2. 批准是原子条件更新：并发批准只有第一个生效（rowcount 决定）。
3. 提交闸：任何时刻至多一笔提案处于 SUBMITTING/UNKNOWN；存在 UNKNOWN 时
   必须先核实或重试该笔，否则不放行下一笔。闸检查与状态迁移在同一事务。
4. 所有迁移都是独立短事务；网络等待发生在两次事务之间，绝不持库锁等待。
5. 进程重启后 SUBMITTING 一律恢复为 UNKNOWN（上次提交结果不可知，待核实）。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from typing import Any

# 提案状态
PENDING = "PENDING"        # 待批准
APPROVED = "APPROVED"      # 已批准、待提交
SUBMITTING = "SUBMITTING"  # 已落库为提交中，正在等待上游回执
UNKNOWN = "UNKNOWN"        # 超时/断流：待核实（不得视为成功或失败）
APPLIED = "APPLIED"        # 已生效（上游回执确认）
REJECTED = "REJECTED"      # 已拒绝（上游回执确认，如旧修订）

BLOCKING_STATUSES = (SUBMITTING, UNKNOWN)  # 占用提交闸的状态

SCHEMA = """
CREATE TABLE IF NOT EXISTS proposals (
  proposal_id    TEXT PRIMARY KEY,
  notice_id      TEXT NOT NULL,
  target_profile TEXT NOT NULL CHECK (target_profile IN ('public','personal')),
  base_revision  INTEGER NOT NULL,
  proposer       TEXT NOT NULL,
  approver       TEXT,
  status         TEXT NOT NULL CHECK (status IN
    ('PENDING','APPROVED','SUBMITTING','UNKNOWN','APPLIED','REJECTED')),
  receipt_json   TEXT,        -- 上游回执（最终或最近已知）
  last_error     TEXT,        -- 进入 UNKNOWN 时的传输层信息
  attempts       INTEGER NOT NULL DEFAULT 0,
  created_at     REAL NOT NULL,
  updated_at     REAL NOT NULL,
  submitted_at   REAL
);
"""


class ApprovalError(Exception):
    """状态机拒绝的迁移。携带 HTTP 状态码语义。"""

    def __init__(self, message: str, status_code: int = 409) -> None:
        super().__init__(message)
        self.status_code = status_code


class ApprovalStore:
    """线程安全的 SQLite 提案存储（每个迁移独立短事务）。"""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        parent = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(parent, exist_ok=True)
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
                conn.commit()
            finally:
                conn.close()

    # ---------------- 只读 ----------------

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        d = dict(row)
        d["receipt"] = json.loads(d["receipt_json"]) if d["receipt_json"] else None
        d.pop("receipt_json", None)
        return d

    def get(self, proposal_id: str) -> dict[str, Any] | None:
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT * FROM proposals WHERE proposal_id = ?",
                    (proposal_id,),
                ).fetchone()
            finally:
                conn.close()
        return self._row_to_dict(row) if row else None

    def list_all(self) -> list[dict[str, Any]]:
        with self._lock:
            conn = self._connect()
            try:
                rows = conn.execute(
                    "SELECT * FROM proposals ORDER BY created_at DESC"
                ).fetchall()
            finally:
                conn.close()
        return [self._row_to_dict(r) for r in rows]

    def blocking_proposal(self) -> dict[str, Any] | None:
        """当前占用提交闸的提案（SUBMITTING/UNKNOWN），无则 None。"""
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT * FROM proposals WHERE status IN ('SUBMITTING','UNKNOWN')"
                    " ORDER BY updated_at LIMIT 1"
                ).fetchone()
            finally:
                conn.close()
        return self._row_to_dict(row) if row else None

    # ---------------- 迁移 ----------------

    def create(
        self,
        *,
        notice_id: str,
        target_profile: str,
        base_revision: int,
        proposer: str,
    ) -> dict[str, Any]:
        if target_profile not in ("public", "personal"):
            raise ApprovalError("target_profile 必须是 public|personal", 400)
        if not notice_id:
            raise ApprovalError("notice_id 不能为空", 400)
        if int(base_revision) < 1:
            raise ApprovalError("base_revision 非法", 400)
        now = time.time()
        proposal_id = uuid.uuid4().hex
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "INSERT INTO proposals"
                    "(proposal_id, notice_id, target_profile, base_revision,"
                    " proposer, status, attempts, created_at, updated_at)"
                    " VALUES(?,?,?,?,?,?,0,?,?)",
                    (
                        proposal_id,
                        notice_id,
                        target_profile,
                        int(base_revision),
                        proposer,
                        PENDING,
                        now,
                        now,
                    ),
                )
                conn.commit()
            finally:
                conn.close()
        created = self.get(proposal_id)
        assert created is not None
        return created

    def approve(self, proposal_id: str, approver: str) -> dict[str, Any]:
        """批准：原子条件更新，并发批准只有一笔生效；发起人不得批准。"""
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT proposer, status FROM proposals WHERE proposal_id = ?",
                    (proposal_id,),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    raise ApprovalError("提案不存在", 404)
                if row["proposer"] == approver:
                    conn.rollback()
                    raise ApprovalError("发起人不能批准自己的提案，须另一人复核", 403)
                cur = conn.execute(
                    "UPDATE proposals SET status = ?, approver = ?, updated_at = ?"
                    " WHERE proposal_id = ? AND status = ?",
                    (APPROVED, approver, time.time(), proposal_id, PENDING),
                )
                if cur.rowcount != 1:
                    conn.rollback()
                    raise ApprovalError(
                        f"提案当前状态为 {row['status']}，批准未生效"
                        "（可能已被他人并发处理）"
                    )
                conn.commit()
            finally:
                conn.close()
        updated = self.get(proposal_id)
        assert updated is not None
        return updated

    def begin_submit(self, proposal_id: str) -> dict[str, Any]:
        """进入提交中：先落库 SUBMITTING，再允许发起上游请求。

        同一事务内完成提交闸检查与状态迁移；允许 APPROVED 首提或 UNKNOWN 重试
        （重试沿用同一 proposal_id，上游按 ID 去重保证不重复生效）。
        """
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT status FROM proposals WHERE proposal_id = ?",
                    (proposal_id,),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    raise ApprovalError("提案不存在", 404)
                if row["status"] not in (APPROVED, UNKNOWN):
                    conn.rollback()
                    raise ApprovalError(
                        f"提案当前状态为 {row['status']}，不能提交"
                    )
                blocker = conn.execute(
                    "SELECT proposal_id, status FROM proposals"
                    " WHERE status IN ('SUBMITTING','UNKNOWN')"
                    " AND proposal_id != ? LIMIT 1",
                    (proposal_id,),
                ).fetchone()
                if blocker is not None:
                    conn.rollback()
                    raise ApprovalError(
                        f"提案 {blocker['proposal_id'][:8]} 处于"
                        f" {blocker['status']}，须先核实/完成该笔，"
                        "不得放行下一笔"
                    )
                now = time.time()
                cur = conn.execute(
                    "UPDATE proposals SET status = ?, attempts = attempts + 1,"
                    " submitted_at = ?, updated_at = ?, last_error = NULL"
                    " WHERE proposal_id = ? AND status IN (?, ?)",
                    (SUBMITTING, now, now, proposal_id, APPROVED, UNKNOWN),
                )
                if cur.rowcount != 1:
                    conn.rollback()
                    raise ApprovalError("提交状态迁移冲突，请刷新后重试")
                conn.commit()
            finally:
                conn.close()
        updated = self.get(proposal_id)
        assert updated is not None
        return updated

    def _finish(
        self,
        proposal_id: str,
        status: str,
        *,
        receipt: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "UPDATE proposals SET status = ?, receipt_json = ?,"
                    " last_error = ?, updated_at = ?"
                    " WHERE proposal_id = ? AND status IN ('SUBMITTING','UNKNOWN')",
                    (
                        status,
                        json.dumps(receipt) if receipt is not None else None,
                        error,
                        time.time(),
                        proposal_id,
                    ),
                )
                conn.commit()
            finally:
                conn.close()
        updated = self.get(proposal_id)
        assert updated is not None
        return updated

    def finish_applied(
        self, proposal_id: str, receipt: dict[str, Any]
    ) -> dict[str, Any]:
        return self._finish(proposal_id, APPLIED, receipt=receipt)

    def finish_rejected(
        self, proposal_id: str, receipt: dict[str, Any]
    ) -> dict[str, Any]:
        return self._finish(proposal_id, REJECTED, receipt=receipt)

    def mark_unknown(self, proposal_id: str, error: str) -> dict[str, Any]:
        """超时/断流：标记待核实。不得在此判定成功或失败。"""
        return self._finish(proposal_id, UNKNOWN, error=error)

    def resolve_from_receipt(
        self, proposal_id: str, receipt: dict[str, Any]
    ) -> dict[str, Any]:
        """核实：以上游持久化的回执为准收敛本地状态。"""
        outcome = receipt.get("outcome")
        if outcome == "applied":
            return self._finish(proposal_id, APPLIED, receipt=receipt)
        if outcome == "rejected":
            return self._finish(proposal_id, REJECTED, receipt=receipt)
        raise ApprovalError(f"上游回执 outcome 非法: {outcome!r}", 502)

    # ---------------- 崩溃恢复 ----------------

    def recover_after_restart(self) -> list[str]:
        """进程重启：SUBMITTING -> UNKNOWN（结果不可知，待核实）。返回迁移的 ID。"""
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                rows = conn.execute(
                    "SELECT proposal_id FROM proposals WHERE status = 'SUBMITTING'"
                ).fetchall()
                ids = [r["proposal_id"] for r in rows]
                conn.execute(
                    "UPDATE proposals SET status = 'UNKNOWN',"
                    " last_error = COALESCE(last_error, '') ||"
                    " ' [进程重启，上次提交结果待核实]', updated_at = ?"
                    " WHERE status = 'SUBMITTING'",
                    (time.time(),),
                )
                conn.commit()
            finally:
                conn.close()
        return ids
