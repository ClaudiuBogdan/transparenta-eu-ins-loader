/**
 * SyncQueueService - Unified sync task and chunk management
 *
 * Provides a single entry point for all sync operations with:
 * - Task-level management (matrix sync requests)
 * - Chunk-level tracking (individual API calls)
 * - Global rate limiting (prevents parallel INS API calls)
 * - Lease-based locking (distributed safety)
 * - Automatic retry with exponential backoff
 */

import { hostname } from "node:os";

import { sql, type Kysely } from "kysely";

import {
  SYNC_DEFAULTS,
  type Database,
  type SyncTask,
  type SyncTaskStatus,
  type SyncCheckpoint,
  type SyncChunkStatus,
  type NewSyncTask,
  type NewSyncCheckpoint,
} from "../../db/types.js";
import { apiLogger } from "../../logger.js";

// ============================================================================
// Types
// ============================================================================

export interface CreateTaskOptions {
  matrixId: number;
  yearFrom?: number;
  yearTo?: number;
  classificationMode?: "totals-only" | "all";
  countyCode?: string;
  priority?: number;
  createdBy: "cli" | "api";
}

export interface ChunkPlan {
  chunkHash: string;
  chunkIndex: number;
  chunkName: string;
  countyCode?: string;
  yearFrom?: number;
  yearTo?: number;
  cellsEstimated: number;
}

export interface ChunkResult {
  cellsReturned: number;
  rowsSynced: number;
}

export interface TaskResult {
  chunksCompleted: number;
  chunksFailed: number;
  rowsInserted: number;
  rowsUpdated: number;
  duration: number;
  errors: string[];
}

export interface SyncSystemStatus {
  tasks: {
    pending: number;
    planning: number;
    running: number;
    completed: number;
    failed: number;
    cancelled: number;
  };
  rateLimiter: {
    isLocked: boolean;
    lockedBy: string | null;
    lastCallAt: Date | null;
    callsToday: number;
  };
}

export interface TaskFilters {
  status?: SyncTaskStatus;
  matrixId?: number;
  createdBy?: "cli" | "api";
  limit?: number;
  offset?: number;
}

export interface CleanupResult {
  tasksReset: number;
  chunksReset: number;
}

export interface SyncTaskWithProgress extends SyncTask {
  matrixCode: string;
  matrixName: string;
  progressPct: number;
}

// ============================================================================
// Constants
// ============================================================================

const { retry: RETRY_CONFIG, lease: LEASE_CONFIG } = SYNC_DEFAULTS;

// Errors that should not be retried
const PERMANENT_ERROR_PATTERNS = [
  /matrix not found/i,
  /partition does not exist/i,
  /invalid.*encquery/i,
  /no data available/i,
];

// ============================================================================
// SyncQueueService
// ============================================================================

export class SyncQueueService {
  private workerId: string;

  constructor(private db: Kysely<Database>) {
    // Generate unique worker ID for this process
    this.workerId = `${hostname()}-${String(process.pid)}-${Math.random().toString(36).slice(2, 8)}`;
  }

  // ==========================================================================
  // Task Management
  // ==========================================================================

  /**
   * Create a new sync task or return existing active task for same parameters.
   */
  async createTask(
    options: CreateTaskOptions
  ): Promise<{ task: SyncTask; isNew: boolean }> {
    const yearFrom = options.yearFrom ?? SYNC_DEFAULTS.yearFrom;
    const yearTo = options.yearTo ?? SYNC_DEFAULTS.yearTo;
    const classificationMode =
      options.classificationMode ?? SYNC_DEFAULTS.classificationMode;
    const countyCode = options.countyCode ?? null;
    const priority = options.priority ?? 0;

    // Check for existing active task with same parameters
    const existingTask = await this.db
      .selectFrom("sync_tasks")
      .selectAll()
      .where("matrix_id", "=", options.matrixId)
      .where("year_from", "=", yearFrom)
      .where("year_to", "=", yearTo)
      .where("classification_mode", "=", classificationMode)
      .where((eb) =>
        countyCode === null
          ? eb("county_code", "is", null)
          : eb("county_code", "=", countyCode)
      )
      .where("status", "in", ["PENDING", "PLANNING", "RUNNING"])
      .executeTakeFirst();

    if (existingTask) {
      return { task: existingTask, isNew: false };
    }

    // Create new task
    const newTask: NewSyncTask = {
      matrix_id: options.matrixId,
      year_from: yearFrom,
      year_to: yearTo,
      classification_mode: classificationMode,
      county_code: countyCode,
      status: "PENDING",
      priority,
      chunks_completed: 0,
      chunks_failed: 0,
      rows_inserted: 0,
      rows_updated: 0,
      created_by: options.createdBy,
    };

    const task = await this.db
      .insertInto("sync_tasks")
      .values(newTask)
      .returningAll()
      .executeTakeFirstOrThrow();

    apiLogger.info(
      { taskId: task.id, matrixId: options.matrixId, yearFrom, yearTo },
      "Created sync task"
    );

    return { task, isNew: true };
  }

  /**
   * Claim next available task for processing.
   * Automatically cleans up stale locks first.
   */
  async claimTask(): Promise<SyncTask | null> {
    // Clean up stale locks first
    await this.cleanupStaleLocks();

    const leaseExpiry = new Date(Date.now() + LEASE_CONFIG.taskDurationMs);

    // Claim task using UPDATE with RETURNING (atomic operation)
    const claimed = await this.db
      .updateTable("sync_tasks")
      .set({
        status: "PLANNING",
        locked_until: leaseExpiry,
        locked_by: this.workerId,
        started_at: sql`COALESCE(started_at, NOW())`,
      })
      .where("id", "=", (eb) =>
        eb
          .selectFrom("sync_tasks")
          .select("id")
          .where("status", "=", "PENDING")
          .orderBy("priority", "desc")
          .orderBy("created_at", "asc")
          .limit(1)
      )
      .returningAll()
      .executeTakeFirst();

    if (claimed) {
      apiLogger.info(
        { taskId: claimed.id, matrixId: claimed.matrix_id },
        "Claimed sync task"
      );
    }

    return claimed ?? null;
  }

  /**
   * Get a specific task by ID.
   */
  async getTask(taskId: number): Promise<SyncTask | null> {
    const result = await this.db
      .selectFrom("sync_tasks")
      .selectAll()
      .where("id", "=", taskId)
      .executeTakeFirst();
    return result ?? null;
  }

  /**
   * Get task with matrix info and progress percentage.
   */
  async getTaskWithProgress(
    taskId: number
  ): Promise<SyncTaskWithProgress | null> {
    const result = await this.db
      .selectFrom("sync_tasks")
      .innerJoin("matrices", "sync_tasks.matrix_id", "matrices.id")
      .select([
        "sync_tasks.id",
        "sync_tasks.matrix_id",
        "sync_tasks.year_from",
        "sync_tasks.year_to",
        "sync_tasks.classification_mode",
        "sync_tasks.county_code",
        "sync_tasks.status",
        "sync_tasks.priority",
        "sync_tasks.chunks_total",
        "sync_tasks.chunks_completed",
        "sync_tasks.chunks_failed",
        "sync_tasks.rows_inserted",
        "sync_tasks.rows_updated",
        "sync_tasks.created_at",
        "sync_tasks.started_at",
        "sync_tasks.completed_at",
        "sync_tasks.error_message",
        "sync_tasks.locked_until",
        "sync_tasks.locked_by",
        "sync_tasks.created_by",
        "matrices.ins_code",
        "matrices.metadata",
      ])
      .where("sync_tasks.id", "=", taskId)
      .executeTakeFirst();

    if (!result) return null;

    const matrixName =
      (result.metadata as { names?: { ro?: string } })?.names?.ro ??
      result.ins_code;
    const progressPct =
      result.chunks_total && result.chunks_total > 0
        ? Math.round((result.chunks_completed / result.chunks_total) * 100)
        : 0;

    return {
      id: result.id,
      matrix_id: result.matrix_id,
      year_from: result.year_from,
      year_to: result.year_to,
      classification_mode: result.classification_mode,
      county_code: result.county_code,
      status: result.status,
      priority: result.priority,
      chunks_total: result.chunks_total,
      chunks_completed: result.chunks_completed,
      chunks_failed: result.chunks_failed,
      rows_inserted: result.rows_inserted,
      rows_updated: result.rows_updated,
      created_at: result.created_at,
      started_at: result.started_at,
      completed_at: result.completed_at,
      error_message: result.error_message,
      locked_until: result.locked_until,
      locked_by: result.locked_by,
      created_by: result.created_by,
      matrixCode: result.ins_code,
      matrixName,
      progressPct,
    };
  }

  /**
   * List tasks with optional filters.
   */
  async listTasks(filters?: TaskFilters): Promise<SyncTask[]> {
    let query = this.db
      .selectFrom("sync_tasks")
      .selectAll()
      .orderBy("created_at", "desc");

    if (filters?.status) {
      query = query.where("status", "=", filters.status);
    }
    if (filters?.matrixId) {
      query = query.where("matrix_id", "=", filters.matrixId);
    }
    if (filters?.createdBy) {
      query = query.where("created_by", "=", filters.createdBy);
    }
    if (filters?.limit) {
      query = query.limit(filters.limit);
    }
    if (filters?.offset) {
      query = query.offset(filters.offset);
    }

    return query.execute();
  }

  /**
   * Save generated chunks and transition task to RUNNING status.
   */
  async saveChunks(taskId: number, chunks: ChunkPlan[]): Promise<void> {
    const task = await this.getTask(taskId);
    if (!task) {
      throw new Error(`Task ${String(taskId)} not found`);
    }

    // Insert all chunks
    if (chunks.length > 0) {
      const chunkRecords: NewSyncCheckpoint[] = chunks.map((chunk) => ({
        task_id: taskId,
        matrix_id: task.matrix_id,
        chunk_hash: chunk.chunkHash,
        chunk_index: chunk.chunkIndex,
        chunk_name: chunk.chunkName,
        county_code: chunk.countyCode ?? null,
        year_from: chunk.yearFrom ?? null,
        year_to: chunk.yearTo ?? null,
        cells_estimated: chunk.cellsEstimated,
        status: "PENDING" as SyncChunkStatus,
        retry_count: 0,
      }));

      await this.db
        .insertInto("sync_checkpoints")
        .values(chunkRecords)
        .onConflict((oc) =>
          oc.columns(["matrix_id", "chunk_hash"]).doUpdateSet({
            task_id: taskId,
            chunk_index: sql`EXCLUDED.chunk_index`,
            chunk_name: sql`EXCLUDED.chunk_name`,
            cells_estimated: sql`EXCLUDED.cells_estimated`,
            status: "PENDING",
            error_message: null,
            retry_count: 0,
            next_retry_at: null,
          })
        )
        .execute();
    }

    // Update task to RUNNING with chunk count
    await this.db
      .updateTable("sync_tasks")
      .set({
        status: "RUNNING",
        chunks_total: chunks.length,
        locked_until: new Date(Date.now() + LEASE_CONFIG.taskDurationMs),
      })
      .where("id", "=", taskId)
      .execute();

    apiLogger.info(
      { taskId, chunkCount: chunks.length },
      "Saved chunks and transitioned task to RUNNING"
    );
  }

  /**
   * Update task progress (called after each chunk completes).
   */
  async updateTaskProgress(
    taskId: number,
    delta: {
      rowsInserted?: number;
      rowsUpdated?: number;
      chunksTotal?: number;
    }
  ): Promise<void> {
    const updates: Record<string, unknown> = {
      rows_inserted: sql`rows_inserted + ${delta.rowsInserted ?? 0}`,
      rows_updated: sql`rows_updated + ${delta.rowsUpdated ?? 0}`,
      // Refresh lease
      locked_until: new Date(Date.now() + LEASE_CONFIG.taskDurationMs),
    };

    // Set chunks_total if provided (only set once, not incremented)
    if (delta.chunksTotal !== undefined) {
      updates.chunks_total = delta.chunksTotal;
    }

    await this.db
      .updateTable("sync_tasks")
      .set(updates)
      .where("id", "=", taskId)
      .execute();
  }

  /**
   * Mark task as completed.
   */
  async completeTask(taskId: number): Promise<void> {
    // Get final counts from checkpoints
    const counts = await this.db
      .selectFrom("sync_checkpoints")
      .select([
        sql<number>`COUNT(*) FILTER (WHERE status = 'COMPLETED')`.as(
          "completed"
        ),
        sql<number>`COUNT(*) FILTER (WHERE status IN ('FAILED', 'EXHAUSTED'))`.as(
          "failed"
        ),
      ])
      .where("task_id", "=", taskId)
      .executeTakeFirst();

    await this.db
      .updateTable("sync_tasks")
      .set({
        status: "COMPLETED",
        completed_at: new Date(),
        chunks_completed: counts?.completed ?? 0,
        chunks_failed: counts?.failed ?? 0,
        locked_until: null,
        locked_by: null,
      })
      .where("id", "=", taskId)
      .execute();

    apiLogger.info({ taskId }, "Task completed");
  }

  /**
   * Mark task as failed.
   */
  async failTask(taskId: number, error: string): Promise<void> {
    await this.db
      .updateTable("sync_tasks")
      .set({
        status: "FAILED",
        completed_at: new Date(),
        error_message: error.substring(0, 1000),
        locked_until: null,
        locked_by: null,
      })
      .where("id", "=", taskId)
      .execute();

    apiLogger.error({ taskId, error }, "Task failed");
  }

  /**
   * Cancel a pending or running task.
   */
  async cancelTask(taskId: number): Promise<boolean> {
    const result = await this.db
      .updateTable("sync_tasks")
      .set({
        status: "CANCELLED",
        completed_at: new Date(),
        locked_until: null,
        locked_by: null,
      })
      .where("id", "=", taskId)
      .where("status", "in", ["PENDING", "PLANNING", "RUNNING"])
      .executeTakeFirst();

    const cancelled = (result.numUpdatedRows ?? 0n) > 0n;
    if (cancelled) {
      apiLogger.info({ taskId }, "Task cancelled");
    }
    return cancelled;
  }

  /**
   * Reset a failed task for retry.
   */
  async retryTask(taskId: number): Promise<boolean> {
    const result = await this.db
      .updateTable("sync_tasks")
      .set({
        status: "PENDING",
        completed_at: null,
        error_message: null,
        locked_until: null,
        locked_by: null,
      })
      .where("id", "=", taskId)
      .where("status", "=", "FAILED")
      .executeTakeFirst();

    const reset = (result.numUpdatedRows ?? 0n) > 0n;

    if (reset) {
      // Also reset exhausted chunks
      await this.db
        .updateTable("sync_checkpoints")
        .set({
          status: "PENDING",
          error_message: null,
          retry_count: 0,
          next_retry_at: null,
          locked_until: null,
          locked_by: null,
        })
        .where("task_id", "=", taskId)
        .where("status", "in", ["FAILED", "EXHAUSTED"])
        .execute();

      apiLogger.info({ taskId }, "Task reset for retry");
    }

    return reset;
  }

  /**
   * Retry all failed tasks.
   */
  async retryAllFailedTasks(): Promise<number> {
    const failedTasks = await this.db
      .selectFrom("sync_tasks")
      .select("id")
      .where("status", "=", "FAILED")
      .execute();

    let count = 0;
    for (const task of failedTasks) {
      const success = await this.retryTask(task.id);
      if (success) count++;
    }

    return count;
  }

  // ==========================================================================
  // Chunk Management
  // ==========================================================================

  /**
   * Claim next available chunk within a task.
   */
  async claimChunk(taskId: number): Promise<SyncCheckpoint | null> {
    const now = new Date();
    const leaseExpiry = new Date(Date.now() + LEASE_CONFIG.chunkDurationMs);

    // Try to claim a PENDING chunk first, then FAILED chunks that are ready to retry
    const claimed = await this.db
      .updateTable("sync_checkpoints")
      .set({
        status: "RUNNING",
        locked_until: leaseExpiry,
        locked_by: this.workerId,
        started_at: sql`COALESCE(started_at, NOW())`,
      })
      .where("id", "=", (eb) =>
        eb
          .selectFrom("sync_checkpoints")
          .select("id")
          .where("task_id", "=", taskId)
          .where((wb) =>
            wb.or([
              wb("status", "=", "PENDING"),
              wb.and([
                wb("status", "=", "FAILED"),
                wb.or([
                  wb("next_retry_at", "is", null),
                  wb("next_retry_at", "<=", now),
                ]),
              ]),
            ])
          )
          .orderBy("chunk_index", "asc")
          .limit(1)
      )
      .returningAll()
      .executeTakeFirst();

    return claimed ?? null;
  }

  /**
   * Mark chunk as completed.
   */
  async completeChunk(chunkId: number, result: ChunkResult): Promise<void> {
    await this.db
      .updateTable("sync_checkpoints")
      .set({
        status: "COMPLETED",
        cells_returned: result.cellsReturned,
        rows_synced: result.rowsSynced,
        completed_at: new Date(),
        error_message: null,
        locked_until: null,
        locked_by: null,
      })
      .where("id", "=", chunkId)
      .execute();

    // Update parent task's completed count
    const chunk = await this.db
      .selectFrom("sync_checkpoints")
      .select("task_id")
      .where("id", "=", chunkId)
      .executeTakeFirst();

    if (chunk) {
      await this.db
        .updateTable("sync_tasks")
        .set({
          chunks_completed: sql`chunks_completed + 1`,
        })
        .where("id", "=", chunk.task_id)
        .execute();
    }
  }

  /**
   * Mark chunk as failed with retry logic.
   */
  async failChunk(
    chunkId: number,
    error: string
  ): Promise<{ willRetry: boolean; isExhausted: boolean }> {
    const chunk = await this.db
      .selectFrom("sync_checkpoints")
      .select(["task_id", "retry_count"])
      .where("id", "=", chunkId)
      .executeTakeFirst();

    if (!chunk) {
      return { willRetry: false, isExhausted: false };
    }

    const newRetryCount = chunk.retry_count + 1;
    const isPermanentError = PERMANENT_ERROR_PATTERNS.some((p) =>
      p.test(error)
    );
    const isExhausted =
      newRetryCount >= RETRY_CONFIG.maxRetries || isPermanentError;

    if (isExhausted) {
      // No more retries
      await this.db
        .updateTable("sync_checkpoints")
        .set({
          status: "EXHAUSTED",
          error_message: error.substring(0, 1000),
          retry_count: newRetryCount,
          completed_at: new Date(),
          locked_until: null,
          locked_by: null,
        })
        .where("id", "=", chunkId)
        .execute();

      // Update parent task's failed count
      await this.db
        .updateTable("sync_tasks")
        .set({
          chunks_failed: sql`chunks_failed + 1`,
        })
        .where("id", "=", chunk.task_id)
        .execute();

      return { willRetry: false, isExhausted: true };
    }

    // Calculate exponential backoff
    const backoffMs = Math.min(
      RETRY_CONFIG.initialBackoffMs *
        Math.pow(RETRY_CONFIG.backoffMultiplier, chunk.retry_count),
      RETRY_CONFIG.maxBackoffMs
    );
    const nextRetryAt = new Date(Date.now() + backoffMs);

    await this.db
      .updateTable("sync_checkpoints")
      .set({
        status: "FAILED",
        error_message: error.substring(0, 1000),
        retry_count: newRetryCount,
        next_retry_at: nextRetryAt,
        locked_until: null,
        locked_by: null,
      })
      .where("id", "=", chunkId)
      .execute();

    apiLogger.warn(
      { chunkId, retryCount: newRetryCount, nextRetryAt, error },
      "Chunk failed, will retry"
    );

    return { willRetry: true, isExhausted: false };
  }

  /**
   * Skip a chunk (mark as already up-to-date).
   */
  async skipChunk(chunkId: number): Promise<void> {
    await this.db
      .updateTable("sync_checkpoints")
      .set({
        status: "SKIPPED",
        completed_at: new Date(),
        locked_until: null,
        locked_by: null,
      })
      .where("id", "=", chunkId)
      .execute();

    // Update parent task's completed count (skipped counts as completed)
    const chunk = await this.db
      .selectFrom("sync_checkpoints")
      .select("task_id")
      .where("id", "=", chunkId)
      .executeTakeFirst();

    if (chunk) {
      await this.db
        .updateTable("sync_tasks")
        .set({
          chunks_completed: sql`chunks_completed + 1`,
        })
        .where("id", "=", chunk.task_id)
        .execute();
    }
  }

  /**
   * Get failed chunks for a task.
   */
  async getFailedChunks(taskId: number): Promise<SyncCheckpoint[]> {
    return this.db
      .selectFrom("sync_checkpoints")
      .selectAll()
      .where("task_id", "=", taskId)
      .where("status", "in", ["FAILED", "EXHAUSTED"])
      .orderBy("chunk_index", "asc")
      .execute();
  }

  /**
   * Check if all chunks for a task are done (completed, skipped, or exhausted).
   */
  async isTaskDone(taskId: number): Promise<boolean> {
    const remaining = await this.db
      .selectFrom("sync_checkpoints")
      .select(sql<number>`COUNT(*)`.as("count"))
      .where("task_id", "=", taskId)
      .where("status", "in", ["PENDING", "RUNNING", "FAILED"])
      .executeTakeFirst();

    return (remaining?.count ?? 0) === 0;
  }

  // ==========================================================================
  // Rate Limiting
  // ==========================================================================

  /**
   * Acquire global API lock. Waits for rate limit if needed.
   */
  async acquireApiLock(timeoutMs = 60000): Promise<void> {
    const startTime = Date.now();

    while (Date.now() - startTime < timeoutMs) {
      const now = new Date();
      const leaseExpiry = new Date(Date.now() + LEASE_CONFIG.apiLockDurationMs);

      // Try to acquire lock
      const result = await this.db
        .updateTable("sync_rate_limiter")
        .set({
          locked_until: leaseExpiry,
          locked_by: this.workerId,
        })
        .where("id", "=", 1)
        .where((eb) =>
          eb.or([eb("locked_until", "is", null), eb("locked_until", "<=", now)])
        )
        .executeTakeFirst();

      if ((result.numUpdatedRows ?? 0n) > 0n) {
        // Got the lock - now check rate limit
        const limiter = await this.db
          .selectFrom("sync_rate_limiter")
          .select(["last_call_at", "min_interval_ms"])
          .where("id", "=", 1)
          .executeTakeFirst();

        if (limiter?.last_call_at) {
          const elapsed = Date.now() - limiter.last_call_at.getTime();
          const waitTime = limiter.min_interval_ms - elapsed;
          if (waitTime > 0) {
            await this.sleep(waitTime);
          }
        }

        return; // Lock acquired
      }

      // Lock is held by another worker, wait and retry
      await this.sleep(100);
    }

    throw new Error(`Timeout waiting for API lock (${String(timeoutMs)}ms)`);
  }

  /**
   * Release global API lock and record the call.
   */
  async releaseApiLock(): Promise<void> {
    const today = new Date().toISOString().split("T")[0];

    await this.db
      .updateTable("sync_rate_limiter")
      .set({
        locked_until: null,
        locked_by: null,
        last_call_at: new Date(),
        calls_today: sql`CASE WHEN stats_reset_at::date = ${today}::date THEN calls_today + 1 ELSE 1 END`,
        stats_reset_at: sql`${today}::date`,
      })
      .where("id", "=", 1)
      .execute();
  }

  // ==========================================================================
  // Monitoring & Cleanup
  // ==========================================================================

  /**
   * Get overall sync system status.
   */
  async getSystemStatus(): Promise<SyncSystemStatus> {
    // Get task counts by status
    const taskCounts = await this.db
      .selectFrom("sync_tasks")
      .select(["status", sql<number>`COUNT(*)::int`.as("count")])
      .groupBy("status")
      .execute();

    const tasks = {
      pending: 0,
      planning: 0,
      running: 0,
      completed: 0,
      failed: 0,
      cancelled: 0,
    };

    for (const row of taskCounts) {
      const key = row.status.toLowerCase() as keyof typeof tasks;
      tasks[key] = row.count;
    }

    // Get rate limiter status
    const limiter = await this.db
      .selectFrom("sync_rate_limiter")
      .select(["locked_until", "locked_by", "last_call_at", "calls_today"])
      .where("id", "=", 1)
      .executeTakeFirst();

    const now = new Date();
    const rateLimiter = {
      isLocked: limiter?.locked_until ? limiter.locked_until > now : false,
      lockedBy: limiter?.locked_by ?? null,
      lastCallAt: limiter?.last_call_at ?? null,
      callsToday: limiter?.calls_today ?? 0,
    };

    return { tasks, rateLimiter };
  }

  /**
   * Clean up stale locks (expired leases).
   */
  async cleanupStaleLocks(): Promise<CleanupResult> {
    const now = new Date();

    // Reset stale tasks (PLANNING or RUNNING with expired lease)
    const taskResult = await this.db
      .updateTable("sync_tasks")
      .set({
        status: "PENDING",
        locked_until: null,
        locked_by: null,
      })
      .where("status", "in", ["PLANNING", "RUNNING"])
      .where("locked_until", "<=", now)
      .executeTakeFirst();

    // Reset stale chunks (RUNNING with expired lease)
    const chunkResult = await this.db
      .updateTable("sync_checkpoints")
      .set({
        status: "PENDING",
        locked_until: null,
        locked_by: null,
      })
      .where("status", "=", "RUNNING")
      .where("locked_until", "<=", now)
      .executeTakeFirst();

    // Reset stale API lock
    await this.db
      .updateTable("sync_rate_limiter")
      .set({
        locked_until: null,
        locked_by: null,
      })
      .where("id", "=", 1)
      .where("locked_until", "<=", now)
      .execute();

    const result = {
      tasksReset: Number(taskResult.numUpdatedRows ?? 0n),
      chunksReset: Number(chunkResult.numUpdatedRows ?? 0n),
    };

    if (result.tasksReset > 0 || result.chunksReset > 0) {
      apiLogger.info(result, "Cleaned up stale locks");
    }

    return result;
  }

  // ==========================================================================
  // Helpers
  // ==========================================================================

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Get the worker ID for this service instance.
   */
  getWorkerId(): string {
    return this.workerId;
  }
}
