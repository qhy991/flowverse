# Resume, safety, and barrier state

## Durable authority

Humanize resumable state is the control authority. The only barrier state it holds is an
append-only list of barrier receipts. Each receipt records every chain's frozen turn count,
whether it was blocked, and the size of its frozen report log. Everything else, such as which
chains are held and whether a session is the first after a barrier, is derived from the
receipts and the chains' turn counts. Actors must not edit control state, report logs,
leaderboard state, manifests, cursors, or anything under `shared/epochs/`.

A barrier is committed only when its receipt is persisted. An epoch directory without a receipt
is discarded on resume, which logs `barrier_epoch_discarded`, and the barrier is frozen again
from the chains' current content. The committed epoch's directories, log sizes, and frozen
leaderboard are checked against its receipt on every layout check, and a mismatch fails closed.
Where GNU `cp --archive` is unavailable, a barrier skips sockets, FIFOs, and device files, and
its `barrier_committed` event lists them in `skipped_special_files`.

The same substantive task resumes compatible state. A run keeps the barrier interval it was
created with, and resuming with a different `barrier_interval` is rejected. A bare `continue`
reads TASK.md when available. If TASK.md changed, the runtime replans over a fresh source
snapshot and unblocks the chains. A chain unblocked this way owes N sessions before the next
barrier, not the sessions it missed while blocked.

There is no barrier timeout. A chain whose session hangs holds every other chain at the barrier
until that session ends.
