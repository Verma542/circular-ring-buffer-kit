"""Core fixed-capacity ring buffer for in-memory log records.

Design choice: we wrap ``collections.deque(maxlen=N)`` rather than hand-rolling
a slot array. ``deque`` already handles eviction, iteration, and len semantics
correctly in CPython. Our value-add is *typed querying* by level and time range,
not re-implementing a circular buffer from scratch. This keeps the surface small
and avoids off-by-one bugs in eviction logic.

Time handling: every ``LogRecord`` carries a monotonic-ascending float supplied
by the caller. Production code passes ``time.monotonic``; tests pass a fake clock
seeded from an integer counter so results are fully deterministic. ``query_range``
*uses the stored value as-is*; if the caller feeds non-monotonic timestamps, the
range filter still works mathematically but the ordering guarantee on ``all()``
is the caller's responsibility. We chose to trust the clock rather than re-sort
on every query, because re-sorting would hide caller bugs.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from typing import Optional

# A clock is any zero-argument callable returning a float used as a timestamp.
# Using a plain ``Callable[[], float]`` alias keeps the API readable without
# pulling in ``Protocol`` machinery that would complicate trivial call sites.
Clock = Callable[[], float]


@dataclass(frozen=True)
class LogRecord:
    """An immutable record stored in :class:`RingBuffer`.

    Frozen so records can be safely shared when returned from queries without
    the caller accidentally mutating the history the ring buffer retains.
    """

    timestamp: float
    level: str
    message: str

    def __iter__(self):
        """Yield (timestamp, level, message) so callers can unpack naturally.

        Mirrors the ``(ts, level, message)`` tuple passed to ``append`` so a
        record round-trips: ``RingBuffer.append(*rec)`` then later
        ``for ts, lvl, msg in buffer.all():`` works identically.
        """
        yield self.timestamp
        yield self.level
        yield self.message


class RingBuffer:
    """Fixed-capacity ring buffer storing the last N log records in memory.

    ``append`` amortised O(1) (deque semantics), ``query_range`` and ``all``
    are O(k) in the number of currently stored records, not capacity. If you
    need sub-linear time lookups by timestamp, this is the wrong structure —
    insert a real log store. This buffer is deliberately for *recent incidents*
    where scanning a few thousand records is fine.
    """

    def __init__(self, capacity: int, clock: Optional[Clock] = None) -> None:
        if not isinstance(capacity, int) or isinstance(capacity, bool):
            raise TypeError("capacity must be an int")
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self._capacity: int = capacity
        self._records: deque[LogRecord] = deque(maxlen=capacity)
        # The default clock is resolved lazily on first append so importing
        # ``time`` happens at call time, not import time — keeps cold imports
        # cheap and lets tests that always pass a fake clock avoid touching
        # the real clock entirely.
        self._clock: Clock = clock if clock is not None else self._default_clock

    @staticmethod
    def _default_clock() -> float:
        """Fallback clock used when the caller omits ``clock`` in tests/REPL."""
        import time

        return time.monotonic()

    @property
    def capacity(self) -> int:
        """Maximum number of records the buffer will retain."""
        return self._capacity

    def __len__(self) -> int:
        return len(self._records)

    def append(self, timestamp: float, level: str, message: str) -> None:
        """Append a ``(timestamp, level, message)`` record.

        ``timestamp`` is taken as authoritative — we deliberately do NOT call
        ``self._clock`` if the caller supplies one, so the same source can feed
        multiple buffers without clock drift. The ``clock`` constructor arg is
        only used when the caller passes ``timestamp=None`` via
        :meth:`append_now`, which is a convenience for ad-hoc usage.
        """
        record = LogRecord(timestamp=float(timestamp), level=level, message=message)
        # ``deque(maxlen=...)`` silently drops the oldest entry when full,
        # which is exactly the eviction policy we want.
        self._records.append(record)

    def append_now(self, level: str, message: str) -> None:
        """Append using the buffer's configured clock for the timestamp.

        Convenience for REPL/debug usage where you do not have a clock value
        handy. In tests, always prefer :meth:`append` with an explicit
        timestamp so results are deterministic.
        """
        self.append(timestamp=self._clock(), level=level, message=message)

    def all(self) -> Iterator[LogRecord]:
        """Iterate over stored records in insertion order (oldest first)."""
        return iter(self._records)

    def query_level(self, level: str) -> list[LogRecord]:
        """Return all records whose ``level`` equals ``level``, oldest first.

        Case-sensitive on purpose — log levels are a closed vocabulary in most
        apps (``INFO``, ``WARNING``, ...) and silent case folding would mask
        typos that a reviewer should see.
        """
        return [rec for rec in self._records if rec.level == level]

    def query_range(
        self, start: float, end: float, level: Optional[str] = None
    ) -> list[LogRecord]:
        """Return records with ``start <= timestamp <= end``, oldest first.

        Inclusive on both ends. If ``level`` is given, additionally filter to
        that level (same case-sensitive semantics as :meth:`query_level`).
        Returning ``end``-inclusive matches how operators read incident windows
        ("show me everything from 10:00 to 10:05" usually means including 10:05).
        """
        if start > end:
            # We treat an inverted range as empty rather than swapping, so the
            # caller finds their bug instead of silently getting data they did
            # not ask for.
            return []
        result: list[LogRecord] = []
        for rec in self._records:
            if rec.timestamp < start or rec.timestamp > end:
                continue
            if level is not None and rec.level != level:
                continue
            result.append(rec)
        return result

    def snapshot(self) -> Sequence[LogRecord]:
        """Return a tuple snapshot of currently stored records.

        A tuple (not a list) because it is immutable and hashable-ish in spirit;
        callers can cache or compare snapshots safely without worrying that a
        later ``append`` will mutate it in place.
        """
        return tuple(self._records)

    def clear(self) -> None:
        """Drop all stored records. Capacity is unchanged."""
        self._records.clear()
