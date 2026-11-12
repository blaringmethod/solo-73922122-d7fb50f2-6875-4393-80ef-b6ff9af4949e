#!/usr/bin/env python3
"""纯单元测试：Cache-Control 解析、缓存资格判定、CacheStore 代际/合并逻辑，
以及双人审批两侧 SQLite 存储（上游 ModeStore / 代理 ApprovalStore）。

运行：python3 -I -m unittest scripts.test_units -v
（不依赖网络与第三方库，存储层仅用 stdlib sqlite3）
"""

import asyncio
import os
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proxy.approval_store import (
    APPROVED,
    APPLIED,
    PENDING,
    REJECTED,
    SUBMITTING,
    UNKNOWN,
    ApprovalError,
    ApprovalStore,
)
from proxy.cachestore import (
    CachedResponse,
    CacheHit,
    CacheLeader,
    CacheStore,
    CacheWait,
)
from proxy.policy import cache_eligibility, parse_cache_control
from upstream.mode_store import ModeStore


class PolicyParseTests(unittest.TestCase):
    def test_basic_directives(self):
        d = parse_cache_control("public, max-age=30, no-transform")
        self.assertEqual(d["public"], True)
        self.assertEqual(d["no-transform"], True)
        self.assertEqual(d["max-age"], "30")

    def test_case_insensitive_and_whitespace(self):
        d = parse_cache_control(" Public , MAX-AGE = 60 ")
        self.assertIn("public", d)
        self.assertEqual(d["max-age"], "60")

    def test_quoted_value(self):
        d = parse_cache_control('public, max-age="45"')
        self.assertEqual(d["max-age"], "45")

    def test_empty(self):
        self.assertEqual(parse_cache_control(None), {})
        self.assertEqual(parse_cache_control(""), {})


class EligibilityTests(unittest.TestCase):
    @staticmethod
    def _hdrs(cc=None, set_cookie=False):
        h = []
        if cc is not None:
            h.append(("Cache-Control", cc))
        if set_cookie:
            h.append(("Set-Cookie", "SID=x"))
        return h

    def test_eligible(self):
        ok, reason, age = cache_eligibility(200, self._hdrs("public, max-age=30"))
        self.assertTrue(ok)
        self.assertEqual(age, 30)

    def test_zero_max_age_allowed_but_immediately_stale(self):
        ok, _, age = cache_eligibility(200, self._hdrs("public, max-age=0"))
        self.assertTrue(ok)
        self.assertEqual(age, 0)

    def test_non_200(self):
        ok, _, age = cache_eligibility(404, self._hdrs("public, max-age=30"))
        self.assertFalse(ok)
        self.assertIsNone(age)

    def test_set_cookie_blocks(self):
        ok, reason, _ = cache_eligibility(
            200, self._hdrs("public, max-age=30", set_cookie=True)
        )
        self.assertFalse(ok)
        self.assertIn("Set-Cookie", reason)

    def test_missing_cc(self):
        ok, _, _ = cache_eligibility(200, self._hdrs(None))
        self.assertFalse(ok)

    def test_private_no_store(self):
        for cc in (
            "private, max-age=30",
            "public, max-age=30, no-store",
            "no-store",
            "public, private, max-age=30",
        ):
            ok, _, _ = cache_eligibility(200, self._hdrs(cc))
            self.assertFalse(ok, cc)

    def test_bad_max_age(self):
        for cc in (
            "public",
            "public, max-age",
            "public, max-age=-1",
            "public, max-age=1.0",
            "public, max-age=abc",
            "public, max-age=",
            "public, max-age=30, max-age=60",
        ):
            ok, _, _ = cache_eligibility(200, self._hdrs(cc))
            self.assertFalse(ok, cc)

    def test_multiple_cc_headers_merged(self):
        headers = [("Cache-Control", "public"), ("Cache-Control", "max-age=12")]
        ok, _, age = cache_eligibility(200, headers)
        self.assertTrue(ok)
        self.assertEqual(age, 12)
        headers2 = [
            ("Cache-Control", "public, max-age=12"),
            ("Cache-Control", "no-store"),
        ]
        ok2, _, _ = cache_eligibility(200, headers2)
        self.assertFalse(ok2)
        # 同一带值指令重复出现属非法，拒绝缓存
        headers3 = [
            ("Cache-Control", "public, max-age=12"),
            ("Cache-Control", "max-age=99"),
        ]
        ok3, reason, _ = cache_eligibility(200, headers3)
        self.assertFalse(ok3)
        self.assertIn("max-age", reason)


class CacheStoreTests(unittest.TestCase):
    def setUp(self):
        self.store = CacheStore()

    @staticmethod
    def _entry(gen=0, ttl=30):
        import time

        now = time.monotonic()
        return CachedResponse(
            status_code=200,
            body=b"{}",
            headers=[(b"content-type", b"application/json")],
            cached_at=now,
            expires_at=now + ttl,
            max_age=ttl,
            generation=gen,
        )

    def _run(self, coro):
        return asyncio.run(coro)

    def test_first_lookup_is_leader_second_waits(self):
        async def scenario():
            key = self.store.make_key("1", "zh")
            d1 = await self.store.lookup(key, "1")
            d2 = await self.store.lookup(key, "1")
            self.assertIsInstance(d1, CacheLeader)
            self.assertIsInstance(d2, CacheWait)
            self.assertIs(d1.inflight, d2.inflight)
            return d1.inflight

        inflight = self._run(scenario())
        self.assertIsNotNone(inflight)

    def test_purge_blocks_backfill_and_new_lookup_is_new_generation(self):
        async def scenario():
            key = self.store.make_key("7", "zh")
            leader = await self.store.lookup(key, "7")
            inflight = leader.inflight
            info = await self.store.purge_notice("7")
            self.assertEqual(info["generation"], 1)
            self.assertEqual(info["marked_inflight"], 1)
            # 旧在途即使带了可缓存条目也禁止回填
            stored = await self.store.store_if_current(inflight, self._entry(0))
            self.assertFalse(stored)
            # 清除后的新 lookup 是新一代的 leader，不复用旧在途
            d2 = await self.store.lookup(key, "7")
            self.assertIsInstance(d2, CacheLeader)
            self.assertEqual(d2.inflight.generation, 1)
            self.assertIsNot(d2.inflight, inflight)

        self._run(scenario())

    def test_purge_removes_all_languages(self):
        async def scenario():
            for lang in ("zh", "en", "fr"):
                key = self.store.make_key("9", lang)
                leader = await self.store.lookup(key, "9")
                await self.store.store_if_current(
                    leader.inflight, self._entry()
                )
            # 另一个公告不应受影响
            other = self.store.make_key("10", "zh")
            lo = await self.store.lookup(other, "10")
            await self.store.store_if_current(lo.inflight, self._entry())
            info = await self.store.purge_notice("9")
            self.assertEqual(info["purged_cache_entries"], 3)
            snap = await self.store.snapshot()
            ids = {c["notice_id"] for c in snap["cache"]}
            self.assertEqual(ids, {"10"})

        self._run(scenario())

    def test_expired_entry_not_returned(self):
        import time

        async def scenario():
            key = self.store.make_key("5", "zh")
            leader = await self.store.lookup(key, "5")
            entry = self._entry(ttl=0)
            entry.cached_at = time.monotonic() - 1
            entry.expires_at = time.monotonic() - 0.01
            await self.store.store_if_current(leader.inflight, entry)
            decision = await self.store.lookup(key, "5")
            self.assertIsInstance(decision, CacheLeader)  # 过期 -> 新 leader

        self._run(scenario())

    def test_failure_drops_inflight_and_future_raises(self):
        async def scenario():
            key = self.store.make_key("6", "zh")
            leader = await self.store.lookup(key, "6")
            await self.store.fail_all(leader.inflight, RuntimeError("boom"))
            snap = await self.store.snapshot()
            self.assertEqual(snap["inflight"], [])
            with self.assertRaises(RuntimeError):
                await leader.inflight.future

        self._run(scenario())


class ModeStoreTests(unittest.TestCase):
    """上游受控配置与回执：去重、修订核对、同事务持久化、重启保留。"""

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.dir.name, "upstream.db")
        self.store = ModeStore(self.db)

    def tearDown(self):
        self.dir.cleanup()

    @staticmethod
    def _proposal(pid="p1", notice="1", target="personal", rev=1):
        return {
            "proposal_id": pid,
            "notice_id": notice,
            "target_profile": target,
            "base_revision": rev,
            "proposer": "alice",
            "approver": "bob",
        }

    def test_initial_state(self):
        cur = self.store.current()
        self.assertEqual(cur["revision"], 1)
        self.assertEqual(cur["default_profile"], "public")
        self.assertEqual(cur["overrides"], {})

    def test_apply_bumps_revision_and_persists_receipt(self):
        receipt, status, deduped = self.store.apply(self._proposal())
        self.assertEqual((status, deduped), (200, False))
        self.assertEqual(receipt["outcome"], "applied")
        self.assertEqual(receipt["new_revision"], 2)
        cur = self.store.current()
        self.assertEqual(cur["revision"], 2)
        self.assertEqual(cur["overrides"], {"1": "personal"})
        self.assertEqual(self.store.profile_for("1"), "personal")
        self.assertEqual(self.store.profile_for("2"), "public")
        # 回执可按提案 ID 核实
        self.assertEqual(self.store.get_receipt("p1"), receipt)

    def test_retry_same_proposal_returns_original_receipt(self):
        r1, _, _ = self.store.apply(self._proposal())
        r2, status2, deduped2 = self.store.apply(self._proposal())
        self.assertTrue(deduped2)
        self.assertEqual(status2, 200)
        self.assertEqual(r1, r2)  # 原回执原样返回
        self.assertEqual(self.store.current()["revision"], 2)  # 不重复生效

    def test_stale_revision_rejected_and_persisted(self):
        self.store.apply(self._proposal())  # rev -> 2
        receipt, status, deduped = self.store.apply(
            self._proposal(pid="p2", notice="2", rev=1)
        )
        self.assertEqual((status, deduped), (409, False))
        self.assertEqual(receipt["outcome"], "rejected")
        self.assertEqual(receipt["reason"], "stale_revision")
        self.assertEqual(self.store.profile_for("2"), "public")  # 未生效
        # 拒绝回执同样幂等
        receipt2, status2, deduped2 = self.store.apply(
            self._proposal(pid="p2", notice="2", rev=1)
        )
        self.assertTrue(deduped2)
        self.assertEqual((status2, receipt2), (409, receipt))

    def test_persistence_across_reopen(self):
        self.store.apply(self._proposal())
        reopened = ModeStore(self.db)
        self.assertEqual(reopened.current()["revision"], 2)
        self.assertEqual(reopened.profile_for("1"), "personal")
        self.assertIsNotNone(reopened.get_receipt("p1"))

    def test_invalid_target_profile_rejected(self):
        with self.assertRaises(ValueError):
            self.store.apply(self._proposal(target="bogus"))


class ApprovalStoreTests(unittest.TestCase):
    """代理提案状态机：原子批准、提交闸、待核实、重启恢复。"""

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.dir.name, "proxy.db")
        self.store = ApprovalStore(self.db)

    def tearDown(self):
        self.dir.cleanup()

    def _create(self, notice="1", target="personal", proposer="alice"):
        return self.store.create(
            notice_id=notice, target_profile=target,
            base_revision=1, proposer=proposer,
        )

    def test_create_pending_and_immutable_columns(self):
        p = self._create()
        self.assertEqual(p["status"], PENDING)
        self.assertEqual(p["approver"], None)
        self.assertEqual(p["attempts"], 0)
        # 存储层不提供任何修改提案内容的方法
        for name in dir(self.store):
            self.assertNotIn(name, ("update", "edit", "modify"))

    def test_proposer_cannot_approve(self):
        p = self._create()
        with self.assertRaises(ApprovalError) as ctx:
            self.store.approve(p["proposal_id"], "alice")
        self.assertEqual(ctx.exception.status_code, 403)

    def test_concurrent_approve_only_one_wins(self):
        p = self._create()
        results = {"ok": 0, "conflict": 0}

        def worker():
            try:
                self.store.approve(p["proposal_id"], "bob")
                results["ok"] += 1
            except ApprovalError:
                results["conflict"] += 1

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads: t.start()
        for t in threads: t.join()
        self.assertEqual(results["ok"], 1)
        self.assertEqual(results["conflict"], 9)
        self.assertEqual(
            self.store.get(p["proposal_id"])["status"], APPROVED
        )

    def test_submit_gate_blocks_next_until_resolved(self):
        p1 = self._create(notice="1")
        p2 = self._create(notice="2")
        self.store.approve(p1["proposal_id"], "bob")
        self.store.approve(p2["proposal_id"], "bob")
        self.store.begin_submit(p1["proposal_id"])
        # 一笔提交中，另一笔不得放行
        with self.assertRaises(ApprovalError):
            self.store.begin_submit(p2["proposal_id"])
        # 待核实同样占闸
        self.store.mark_unknown(p1["proposal_id"], "timeout")
        self.assertEqual(self.store.get(p1["proposal_id"])["status"], UNKNOWN)
        with self.assertRaises(ApprovalError):
            self.store.begin_submit(p2["proposal_id"])
        # 核实收敛后闸释放
        self.store.resolve_from_receipt(
            p1["proposal_id"], {"outcome": "applied", "new_revision": 2}
        )
        self.assertEqual(self.store.get(p1["proposal_id"])["status"], APPLIED)
        self.store.begin_submit(p2["proposal_id"])
        self.assertEqual(
            self.store.get(p2["proposal_id"])["status"], SUBMITTING
        )

    def test_retry_same_proposal_allowed_from_unknown(self):
        p = self._create()
        self.store.approve(p["proposal_id"], "bob")
        self.store.begin_submit(p["proposal_id"])
        self.store.mark_unknown(p["proposal_id"], "ReadTimeout")
        again = self.store.begin_submit(p["proposal_id"])  # 重试原提案
        self.assertEqual(again["attempts"], 2)
        self.assertEqual(again["proposal_id"], p["proposal_id"])

    def test_terminal_states_cannot_resubmit(self):
        p = self._create()
        self.store.approve(p["proposal_id"], "bob")
        self.store.begin_submit(p["proposal_id"])
        self.store.finish_rejected(
            p["proposal_id"], {"outcome": "rejected", "reason": "stale_revision"}
        )
        self.assertEqual(self.store.get(p["proposal_id"])["status"], REJECTED)
        with self.assertRaises(ApprovalError):
            self.store.begin_submit(p["proposal_id"])

    def test_restart_recovers_submitting_to_unknown(self):
        p = self._create()
        self.store.approve(p["proposal_id"], "bob")
        self.store.begin_submit(p["proposal_id"])
        # 模拟进程重启：重新打开同一库
        reopened = ApprovalStore(self.db)
        recovered = reopened.recover_after_restart()
        self.assertEqual(recovered, [p["proposal_id"]])
        self.assertEqual(
            reopened.get(p["proposal_id"])["status"], UNKNOWN
        )
        # 重启后仍可核实/重试
        reopened.resolve_from_receipt(
            p["proposal_id"], {"outcome": "applied", "new_revision": 2}
        )
        self.assertEqual(reopened.get(p["proposal_id"])["status"], APPLIED)

    def test_persistence_across_reopen(self):
        p = self._create()
        self.store.approve(p["proposal_id"], "bob")
        reopened = ApprovalStore(self.db)
        got = reopened.get(p["proposal_id"])
        self.assertEqual(got["status"], APPROVED)
        self.assertEqual(got["approver"], "bob")
        self.assertEqual(got["notice_id"], "1")


if __name__ == "__main__":
    unittest.main(verbosity=2)
