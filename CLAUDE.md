# apalis-redb

An apalis storage backend on redb. Its public API deliberately mirrors `apalis-redis` 1.0.

## Commands

- `cargo test`: integration tests (`tests/backend.rs`) and doctests. README code blocks run as doctests, since README is the crate docs.
- `cargo clippy --all-targets --all-features`: keep this at zero warnings.
- Check feature combinations after touching `src/connection.rs`: `cargo check --no-default-features` and `cargo check --no-default-features --features smol-comp,task-tracking`.

## API parity with apalis-redis

- Before adding or changing a public item, compare it with apalis-redis (source in `~/.cargo/registry/src/*/apalis-redis-1.0.0`). Match its names, signatures and defaults unless redb forces a difference.
- Record every intentional difference in the README section "Differences from `apalis-redis`".

## Architecture

- `RedbStorage` wraps apalis-core's `Persisted<RedbPersistence>`. The queue lifecycle (register, heartbeat, fetch, events, shutdown) is driven by `Persisted`, and `src/persist.rs` only implements the `Persistence` trait.
- Each Redis key in apalis-redis maps to a redb table of the same name (`{queue}:active`, `{queue}:meta`, ...). Names come from the `Config` key methods; typed definitions are in `queries::tables`. The global queue list is the `apalis:queues` table.
- Each Lua script in apalis-redis is one write transaction in `src/queries/`. Keep each operation atomic in a single transaction.

## Rules

- **Never call redb directly from async code.** Wrap the work in `connection::run_blocking`, which uses tokio's blocking pool or the `blocking` crate depending on the feature.
- **Use the `write` / `read` helpers in `src/queries/mod.rs`.** Queue operations pass `config.durability`. Worker register, heartbeat (`keep_alive`) and unregister must pass `Durability::Immediate`, because they flush the non-durable commits made under `Durability::None`. Don't change this without updating the README "Durability" section.
- **Reading a table that may not exist yet:** use `open_read` / `open_read_multimap`, which return `None` instead of an error.
- **Waking workers:** after a commit that makes tasks runnable (push, release, re-enqueue, retry), call `conn.notify(queue)` when `config.emit_events` is set. Workers have no other prompt way to find new work.
- **Timestamps** are Unix seconds, matching apalis-core's `run_at`.
- A stale lock or result (for a task no longer held by the worker) is logged and skipped, not returned as an error. An error would stop the worker.

## Tests

- Each test uses its own `tempfile` database and a unique queue name, and wraps worker runs in `within(secs, ...)` so a hang fails instead of blocking.
- Tests that depend on wakeups or scheduling set a long `heartbeat_interval`, so they can't pass just because a heartbeat woke the worker.
