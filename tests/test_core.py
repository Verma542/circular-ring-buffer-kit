"""Tests for the circular_ring_buffer_kit.core module.

All tests use a fake integer-valued clock or explicit timestamps so nothing
depends on wall-clock time. No sleeps, no floats compared with ``==``.
"""

import unittest

from circular_ring_buffer_kit import RingBuffer, LogRecord


class _FakeClock:
    """Deterministic clock: each call returns the next integer as a float."""

    def __init__(self, start: int = 0) -> None:
        self._n = start

    def __call__(self) -> float:
        v = self._n
        self._n += 1
        return float(v)


class TestConstruction(unittest.TestCase):
    def test_capacity_stored_and_len_starts_at_zero(self):
        b = RingBuffer(5)
        self.assertEqual(b.capacity, 5)
        self.assertEqual(len(b), 0)

    def test_capacity_must_be_positive_int(self):
        with self.assertRaises(ValueError):
            RingBuffer(0)
        with self.assertRaises(ValueError):
            RingBuffer(-3)

    def test_capacity_rejects_bool(self):
        # bool is a subclass of int in Python; we reject it because capacity=1
        # and capacity=True meaning the same thing is a footgun.
        with self.assertRaises(TypeError):
            RingBuffer(True)
        with self.assertRaises(TypeError):
            RingBuffer(False)

    def test_capacity_rejects_non_int(self):
        with self.assertRaises(TypeError):
            RingBuffer(5.0)
        with self.assertRaises(TypeError):
            RingBuffer("5")


class TestAppendAndEviction(unittest.TestCase):
    def test_append_grows_then_evicts_oldest(self):
        b = RingBuffer(3)
        for i in range(5):
            b.append(float(i), "INFO", f"msg-{i}")
        # capacity 3, we appended 5 → only last 3 retained
        self.assertEqual(len(b), 3)
        msgs = [rec.message for rec in b.all()]
        self.assertEqual(msgs, ["msg-2", "msg-3", "msg-4"])

    def test_append_preserves_order_when_under_capacity(self):
        b = RingBuffer(10)
        b.append(1.0, "INFO", "a")
        b.append(2.0, "ERROR", "b")
        b.append(3.0, "INFO", "c")
        self.assertEqual([r.message for r in b.all()], ["a", "b", "c"])

    def test_append_now_uses_clock(self):
        clk = _FakeClock(start=100)
        b = RingBuffer(5, clock=clk)
        b.append_now("INFO", "x")
        b.append_now("ERROR", "y")
        recs = list(b.all())
        self.assertEqual(len(recs), 2)
        self.assertEqual(recs[0].timestamp, 100.0)
        self.assertEqual(recs[1].timestamp, 101.0)
        self.assertEqual(recs[0].level, "INFO")
        self.assertEqual(recs[1].message, "y")

    def test_capacity_one_evicts_each_append(self):
        b = RingBuffer(1)
        b.append(1.0, "INFO", "first")
        b.append(2.0, "ERROR", "second")
        self.assertEqual(len(b), 1)
        only = next(b.all())
        self.assertEqual(only.message, "second")


class TestQueryLevel(unittest.TestCase):
    def test_query_level_returns_only_matching(self):
        b = RingBuffer(10)
        b.append(1.0, "INFO", "i1")
        b.append(2.0, "ERROR", "e1")
        b.append(3.0, "INFO", "i2")
        b.append(4.0, "WARNING", "w1")
        infos = b.query_level("INFO")
        self.assertEqual([r.message for r in infos], ["i1", "i2"])

    def test_query_level_empty_when_no_match(self):
        b = RingBuffer(3)
        b.append(1.0, "INFO", "x")
        self.assertEqual(b.query_level("ERROR"), [])

    def test_query_level_is_case_sensitive(self):
        b = RingBuffer(3)
        b.append(1.0, "INFO", "x")
        self.assertEqual(b.query_level("info"), [])
        self.assertEqual(len(b.query_level("INFO")), 1)


class TestQueryRange(unittest.TestCase):
    def setUp(self):
        self.b = RingBuffer(20)
        # timestamps 0..4, mixed levels
        self.b.append(0.0, "INFO", "m0")
        self.b.append(1.0, "ERROR", "m1")
        self.b.append(2.0, "INFO", "m2")
        self.b.append(3.0, "WARNING", "m3")
        self.b.append(4.0, "INFO", "m4")

    def test_inclusive_on_both_ends(self):
        out = self.b.query_range(1.0, 3.0)
        self.assertEqual([r.message for r in out], ["m1", "m2", "m3"])

    def test_range_single_point(self):
        out = self.b.query_range(2.0, 2.0)
        self.assertEqual([r.message for r in out], ["m2"])

    def test_range_before_all_records(self):
        out = self.b.query_range(-5.0, -1.0)
        self.assertEqual(out, [])

    def test_range_after_all_records(self):
        out = self.b.query_range(5.0, 9.0)
        self.assertEqual(out, [])

    def test_range_with_level_filter(self):
        out = self.b.query_range(0.0, 4.0, level="INFO")
        self.assertEqual([r.message for r in out], ["m0", "m2", "m4"])

    def test_range_with_level_filter_no_match(self):
        out = self.b.query_range(0.0, 4.0, level="DEBUG")
        self.assertEqual(out, [])

    def test_inverted_range_returns_empty(self):
        out = self.b.query_range(3.0, 1.0)
        self.assertEqual(out, [])

    def test_range_full_span(self):
        out = self.b.query_range(0.0, 4.0)
        self.assertEqual([r.message for r in out], ["m0", "m1", "m2", "m3", "m4"])


class TestSnapshotAndClear(unittest.TestCase):
    def test_snapshot_is_independent_of_future_appends(self):
        b = RingBuffer(5)
        b.append(1.0, "INFO", "a")
        snap = b.snapshot()
        b.append(2.0, "INFO", "b")
        # snapshot should still be the one-record tuple from before
        self.assertEqual(len(snap), 1)
        self.assertEqual(snap[0].message, "a")
        # buffer itself now has two
        self.assertEqual(len(b), 2)

    def test_snapshot_is_tuple(self):
        b = RingBuffer(3)
        b.append(1.0, "INFO", "x")
        snap = b.snapshot()
        self.assertIsInstance(snap, tuple)

    def test_clear_empties_but_keeps_capacity(self):
        b = RingBuffer(4)
        b.append(1.0, "INFO", "a")
        b.append(2.0, "INFO", "b")
        self.assertEqual(len(b), 2)
        b.clear()
        self.assertEqual(len(b), 0)
        self.assertEqual(b.capacity, 4)
        # still usable after clear
        b.append(3.0, "INFO", "c")
        self.assertEqual(len(b), 1)
        self.assertEqual(next(b.all()).message, "c")


class TestLogRecord(unittest.TestCase):
    def test_record_is_immutable(self):
        rec = LogRecord(1.0, "INFO", "x")
        with self.assertRaises(Exception):
            rec.level = "ERROR"  # frozen dataclass

    def test_record_iter_roundtrips_append_args(self):
        b = RingBuffer(5)
        b.append(7.0, "WARNING", "hi")
        rec = next(b.all())
        ts, lvl, msg = rec
        self.assertEqual(ts, 7.0)
        self.assertEqual(lvl, "WARNING")
        self.assertEqual(msg, "hi")


class TestEvictionInteractsCorrectlyWithQueries(unittest.TestCase):
    def test_query_after_eviction_does_not_see_dropped_records(self):
        b = RingBuffer(3)
        for i in range(6):
            b.append(float(i), "INFO", f"m{i}")
        # only m3, m4, m5 retained
        self.assertEqual([r.message for r in b.all()], ["m3", "m4", "m5"])
        out = b.query_range(0.0, 2.0)
        self.assertEqual(out, [])
        out2 = b.query_range(3.0, 5.0)
        self.assertEqual([r.message for r in out2], ["m3", "m4", "m5"])


if __name__ == "__main__":
    unittest.main()
