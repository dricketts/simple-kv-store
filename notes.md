LRU list of keys doubles memory footprint because keys get stored twice (index
and LRU list). Can this be optimized?

Checkpointing might run into issues when the log file is not entirely in memory
because checkpoint reads will go to disk. This is problematic because
checkpointing is done in the commit critical section. Maybe there can be a
background thread that pre-fetches values of older keys in the LRU list. This
would need to have a bounded size.

shared_mutex per log slot
either:
- acquire shared_lock on mutex of oldest slot during begin transaction (block
  writer)
- or acquire shared_lock when reading a log slot, check that the logical log
  slot still exists (LogFile maintains some in-memory state for this), and throw
  exception if not (fail readers).

==== Checkpointing
CHECKPOINT_SIZE
MAX_STORE_SIZE