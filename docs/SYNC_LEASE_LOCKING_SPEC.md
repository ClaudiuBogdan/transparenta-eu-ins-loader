# Spec: Auto-Expiring Lease Locks for Sync Operations

## Problem

If a sync process crashes or is killed without updating the DB:
- The chunk/job remains in "RUNNING" state forever
- No other sync can process that chunk
- Manual intervention required to reset state

## Solution: Lease-Based Locking with Auto-Expiry

Replace status-based locking with **time-based leases** that automatically expire.

**Core Idea:**
- "In progress" = `locked_until > NOW()`
- Process must refresh the lease periodically (heartbeat)
- If process dies, lease expires → chunk becomes available
- Final states (COMPLETED, FAILED) are permanent

---

## Schema Design

### Add lease columns to sync tables

```sql
-- sync_checkpoints: track chunk-level progress
ALTER TABLE sync_checkpoints
  ADD COLUMN locked_until TIMESTAMPTZ,
  ADD COLUMN locked_by TEXT;

-- sync_jobs: track job-level progress
ALTER TABLE sync_jobs
  ADD COLUMN locked_until TIMESTAMPTZ,
  ADD COLUMN locked_by TEXT;
```

### State Logic (derived from columns)

```sql
-- Job/Checkpoint states:
-- AVAILABLE:  locked_until IS NULL AND completed_at IS NULL AND failed_at IS NULL
-- RUNNING:    locked_until > NOW()
-- EXPIRED:    locked_until <= NOW() AND completed_at IS NULL AND failed_at IS NULL
-- COMPLETED:  completed_at IS NOT NULL
-- FAILED:     failed_at IS NOT NULL
```

---

## Lock Operations

### 1. Claim a Job (atomic, skips locked)

```sql
UPDATE sync_jobs
SET
  locked_until = NOW() + INTERVAL '5 minutes',
  locked_by = $worker_id
WHERE id = (
  SELECT id FROM sync_jobs
  WHERE completed_at IS NULL
    AND failed_at IS NULL
    AND (locked_until IS NULL OR locked_until <= NOW())
  ORDER BY priority DESC, created_at ASC
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
RETURNING *;
```

### 2. Heartbeat (extend lease)

```sql
UPDATE sync_jobs
SET locked_until = NOW() + INTERVAL '5 minutes'
WHERE id = $job_id
  AND locked_by = $worker_id
  AND locked_until > NOW();
```

### 3. Complete Successfully

```sql
UPDATE sync_jobs
SET
  completed_at = NOW(),
  locked_until = NULL,
  locked_by = NULL
WHERE id = $job_id;
```

### 4. Mark as Failed

```sql
UPDATE sync_jobs
SET
  failed_at = NOW(),
  error_message = $error,
  locked_until = NULL,
  locked_by = NULL
WHERE id = $job_id;
```

---

## Checkpoint Locking (same pattern)

```sql
-- Claim chunk
UPDATE sync_checkpoints
SET
  locked_until = NOW() + INTERVAL '2 minutes',
  locked_by = $worker_id
WHERE matrix_id = $matrix_id
  AND chunk_hash = $chunk_hash
  AND (locked_until IS NULL OR locked_until <= NOW())
RETURNING *;

-- Heartbeat
UPDATE sync_checkpoints
SET locked_until = NOW() + INTERVAL '2 minutes'
WHERE chunk_hash = $hash AND locked_by = $worker_id;

-- Complete
UPDATE sync_checkpoints
SET
  locked_until = NULL,
  locked_by = NULL,
  last_synced_at = NOW(),
  row_count = $rows
WHERE chunk_hash = $hash;
```

---

## Lease Duration Guidelines

| Resource | Lease Duration | Heartbeat Interval |
|----------|---------------|-------------------|
| sync_jobs | 5 minutes | 1 minute |
| sync_checkpoints | 2 minutes | 30 seconds |

---

## Benefits

1. **Self-healing**: Crashed processes don't block forever
2. **No cleanup needed**: Expired leases automatically become available
3. **Simple queries**: State derived from timestamps
4. **Debuggable**: `locked_by` shows which worker holds the lock
5. **No advisory locks**: Pure SQL, works with any connection pool
