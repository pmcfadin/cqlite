## ADDED Requirements

### Requirement: Python streaming `__next__` releases the GIL during blocking I/O
The Python streaming iterator's `__next__` SHALL execute its blocking buffer-refill
(`receiver.recv().await`, driven via `block_on`) inside `py.allow_threads(...)` so the GIL is released
while it blocks on disk I/O. Other Python threads SHALL be able to make progress during a streaming
iterator's blocking refill. Row construction, span finalization, and error conversion — anything
requiring the `Python<'_>` token — SHALL occur outside the released-GIL section.

#### Scenario: A concurrent Python thread makes progress during streaming iteration
- **WHEN** one Python thread iterates a wide table via `db.execute_streaming(...)` and a second Python thread runs a tight counter-increment loop concurrently
- **THEN** the second thread's counter advances past a nonzero floor of increments *during* the first thread's iteration
- **AND** on the unmodified pre-change code the same test fails (the second thread makes ~no progress while the first blocks)

#### Scenario: The blocking recv runs with the GIL released
- **WHEN** `bindings/python/src/result.rs` `StreamingIterator::__next__` is inspected
- **THEN** the blocking `block_on(iter.next_async())` (buffer-refill `receiver.recv().await`) is invoked inside a `py.allow_threads(...)` closure
- **AND** `Row` construction and span finalization are performed outside that closure, after the GIL is re-acquired

### Requirement: Streaming result correctness is unchanged
The GIL-release restructuring SHALL NOT change the rows returned, their order, or the iterator's
`rows_received` accounting. The buffering/backpressure semantics of the bounded mpsc channel SHALL be
unchanged.

#### Scenario: Streaming yields identical rows and order after the change
- **WHEN** a streaming query is executed after the change
- **THEN** it yields the same rows in the same order as before the change
- **AND** `rows_received` reflects the exact number of rows delivered
- **AND** the existing streaming test suite passes unchanged

### Requirement: No new panics or unwraps on the streaming path
The change SHALL NOT introduce `unwrap()`/`expect()` into library code. A poisoned iterator lock SHALL
surface as a catchable Python exception (`RuntimeError`), not a panic or process abort, and SHALL be
constructed after the GIL is re-acquired.

#### Scenario: A poisoned iterator lock raises a catchable exception
- **WHEN** the iterator's internal lock is poisoned
- **THEN** `__next__` raises a Python `RuntimeError`
- **AND** no `unwrap()`/`expect()` is introduced in the binding library code for this path
