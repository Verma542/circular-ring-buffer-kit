# circular-ring-buffer-kit

A fixed-capacity, in-memory ring buffer for the last N log records, queryable by level and time range. Built on `collections.deque(maxlen=N)` with typed queries layered on top.

## Usage

```python
from circular_ring_buffer_kit import RingBuffer, LogRecord

buf = RingBuffer(capacity=1000)

buf.append(1700.0, "INFO", "server started")
buf.append(1701.0, "WARNING", "slow query")
buf.append(1702.0, "ERROR", "db connection lost")

# everything in a time window
recent = buf.query_range(1700.0, 1702.0)

# everything at a level
errors = buf.query_level("ERROR")

# full snapshot as a tuple (immutable; later appends don't affect it)
snap = buf.snapshot()

# iterate oldest-first
for rec in buf.all():
    print(rec.timestamp, rec.level, rec.message)
```

`RingBuffer(capacity: int, clock=None)` — `clock` is a zero-arg callable returning a float; it is only used by `append_now(level, message)`, not by `append(timestamp, level, message)` which trusts the caller's timestamp. `LogRecord` is a frozen dataclass with `.timestamp`, `.level`, `.message` and is iterable as `(timestamp, level, message)`.

## Why this exists

When something goes wrong in a long-running process, the interesting log lines are almost always the *last few hundred*. Shipping every log line to an aggregator works, but for local debugging during an incident you want the last N records queryable in microseconds, with no network hop and no disk seek. This library is that: a `deque` wrapper that keeps the most recent records and lets you slice them by level or timestamp window.

The trade-off: this is **not** a persistent log store. If the process exits, the buffer is gone. It is also **not** indexed — `query_range` is O(records stored), which is fine for a few thousand entries and wrong for a few million. If you need persistence or indexed time queries, use a real log backend.

## Edge cases worth knowing

- `query_range(start, end)` is **inclusive on both ends**. `query_range(2.0, 2.0)` returns records at exactly `2.0`. An inverted range (`start > end`) returns `[]` rather than swapping the bounds — this surfaces caller bugs instead of hiding them.
- `query_level` is **case-sensitive**. `"INFO"` and `"info"` are different levels. Log levels are usually a closed vocabulary; silent case folding would mask typos.
- `capacity` must be a positive `int`. `bool` is rejected even though it is a subclass of `int` in Python, because `RingBuffer(True)` meaning capacity 1 is a footgun.
- `snapshot()` returns a `tuple`, not a `list`, so it is safe to cache without worrying that a later `append` mutates it in place.
- Eviction is silent: once the buffer is full, the oldest record is dropped on the next `append` with no warning. This is the point of a ring buffer, but worth stating.

## Running the tests

```
PYTHONPATH=src python -m unittest discover -s tests
```

## Performance

The window keeps a bounded buffer, so `push` is constant time and memory does not
grow with the length of the stream. `peak` and `trough` are linear in the window
size, which is the trade that keeps `push` cheap.

