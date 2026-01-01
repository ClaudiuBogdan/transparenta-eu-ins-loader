/**
 * Sync API Routes
 *
 * Task-based sync endpoints for monitoring sync status and triggering on-demand data sync.
 * Uses database as task queue to prevent INS API rate limiting and duplicate requests.
 */

import { Type, type Static } from "@sinclair/typebox";
import { sql } from "kysely";

import { db } from "../../db/connection.js";
import {
  SYNC_DEFAULTS,
  type SyncTask,
  type SyncTaskStatus,
} from "../../db/types.js";
import { SyncQueueService } from "../../services/sync/queue.js";
import { NotFoundError, ValidationError } from "../plugins/error-handler.js";
import { MatrixStatusSchema, PaginationMetaSchema } from "../schemas/common.js";

import type { FastifyInstance } from "fastify";

// Re-export for use in this file
const DEFAULT_YEAR_FROM = SYNC_DEFAULTS.yearFrom;
const DEFAULT_YEAR_TO = SYNC_DEFAULTS.yearTo;

// ============================================================================
// Schemas
// ============================================================================

// Task status enum schema
const SyncTaskStatusSchema = Type.Union([
  Type.Literal("PENDING"),
  Type.Literal("PLANNING"),
  Type.Literal("RUNNING"),
  Type.Literal("COMPLETED"),
  Type.Literal("FAILED"),
  Type.Literal("CANCELLED"),
]);

// GET /sync/status query params
const SyncStatusQuerySchema = Type.Object({
  status: Type.Optional(MatrixStatusSchema),
  hasData: Type.Optional(Type.Boolean()),
  limit: Type.Optional(Type.Number({ minimum: 1, maximum: 500, default: 50 })),
  cursor: Type.Optional(Type.String()),
});

type SyncStatusQuery = Static<typeof SyncStatusQuerySchema>;

// Sync summary schema
const SyncSummarySchema = Type.Object({
  total: Type.Number(),
  pending: Type.Number(),
  syncing: Type.Number(),
  synced: Type.Number(),
  failed: Type.Number(),
  stale: Type.Number(),
  withData: Type.Number(),
});

// Queue summary schema
const QueueSummarySchema = Type.Object({
  pendingTasks: Type.Number(),
  planningTasks: Type.Number(),
  runningTasks: Type.Number(),
});

// Matrix sync status item schema
const MatrixSyncItemSchema = Type.Object({
  insCode: Type.String(),
  name: Type.String(),
  syncStatus: MatrixStatusSchema,
  lastSyncAt: Type.Union([Type.String({ format: "date-time" }), Type.Null()]),
  hasData: Type.Boolean(),
  dataPointCount: Type.Optional(Type.Number()),
  syncError: Type.Union([Type.String(), Type.Null()]),
});

// GET /sync/status response
const SyncStatusResponseSchema = Type.Object({
  data: Type.Object({
    summary: SyncSummarySchema,
    queue: QueueSummarySchema,
    matrices: Type.Array(MatrixSyncItemSchema),
  }),
  meta: Type.Object({
    pagination: PaginationMetaSchema,
  }),
});

// POST /sync/data/:matrixCode params
const SyncDataParamsSchema = Type.Object({
  matrixCode: Type.String(),
});

type SyncDataParams = Static<typeof SyncDataParamsSchema>;

// POST /sync/data/:matrixCode body
const SyncDataBodySchema = Type.Object({
  yearFrom: Type.Optional(Type.Number({ minimum: 1900, maximum: 2100 })),
  yearTo: Type.Optional(Type.Number({ minimum: 1900, maximum: 2100 })),
  classificationMode: Type.Optional(
    Type.Union([Type.Literal("totals-only"), Type.Literal("all")])
  ),
  countyCode: Type.Optional(Type.String()),
  priority: Type.Optional(
    Type.Number({ minimum: -10, maximum: 10, default: 0 })
  ),
});

type SyncDataBody = Static<typeof SyncDataBodySchema>;

// Sync task schema for responses
const SyncTaskSchema = Type.Object({
  id: Type.Number(),
  matrixCode: Type.String(),
  matrixName: Type.String(),
  status: SyncTaskStatusSchema,
  yearFrom: Type.Number(),
  yearTo: Type.Number(),
  classificationMode: Type.String(),
  countyCode: Type.Union([Type.String(), Type.Null()]),
  priority: Type.Number(),
  chunksTotal: Type.Union([Type.Number(), Type.Null()]),
  chunksCompleted: Type.Number(),
  chunksFailed: Type.Number(),
  createdAt: Type.String({ format: "date-time" }),
  startedAt: Type.Union([Type.String({ format: "date-time" }), Type.Null()]),
  completedAt: Type.Union([Type.String({ format: "date-time" }), Type.Null()]),
  rowsInserted: Type.Number(),
  rowsUpdated: Type.Number(),
  errorMessage: Type.Union([Type.String(), Type.Null()]),
});

// POST /sync/data/:matrixCode response
const SyncTriggerResponseSchema = Type.Object({
  data: Type.Object({
    task: SyncTaskSchema,
    isNewTask: Type.Boolean(),
    message: Type.String(),
  }),
});

// GET /sync/tasks/:taskId params
const TaskIdParamsSchema = Type.Object({
  taskId: Type.String(),
});

type TaskIdParams = Static<typeof TaskIdParamsSchema>;

// GET /sync/tasks/:taskId response
const SyncTaskResponseSchema = Type.Object({
  data: SyncTaskSchema,
});

// GET /sync/tasks query params
const SyncTasksQuerySchema = Type.Object({
  status: Type.Optional(SyncTaskStatusSchema),
  matrixCode: Type.Optional(Type.String()),
  limit: Type.Optional(Type.Number({ minimum: 1, maximum: 100, default: 20 })),
  cursor: Type.Optional(Type.String()),
});

type SyncTasksQuery = Static<typeof SyncTasksQuerySchema>;

// GET /sync/tasks response
const SyncTasksListResponseSchema = Type.Object({
  data: Type.Array(SyncTaskSchema),
  meta: Type.Object({
    pagination: PaginationMetaSchema,
  }),
});

// POST /sync/tasks/:taskId/retry response
const TaskRetryResponseSchema = Type.Object({
  data: Type.Object({
    success: Type.Boolean(),
    message: Type.String(),
  }),
});

// ============================================================================
// Helper Functions
// ============================================================================

function formatTask(
  task: SyncTask,
  matrixCode: string,
  matrixName: string
): Static<typeof SyncTaskSchema> {
  return {
    id: task.id,
    matrixCode,
    matrixName,
    status: task.status,
    yearFrom: task.year_from,
    yearTo: task.year_to,
    classificationMode: task.classification_mode,
    countyCode: task.county_code,
    priority: task.priority,
    chunksTotal: task.chunks_total,
    chunksCompleted: task.chunks_completed,
    chunksFailed: task.chunks_failed,
    createdAt: task.created_at.toISOString(),
    startedAt: task.started_at?.toISOString() ?? null,
    completedAt: task.completed_at?.toISOString() ?? null,
    rowsInserted: task.rows_inserted,
    rowsUpdated: task.rows_updated,
    errorMessage: task.error_message,
  };
}

// ============================================================================
// Route Registration
// ============================================================================

export function registerSyncRoutes(app: FastifyInstance): void {
  const queueService = new SyncQueueService(db);

  // GET /sync/status - Get sync status summary and matrix list
  app.get<{ Querystring: SyncStatusQuery }>(
    "/sync/status",
    {
      schema: {
        summary: "Get sync status",
        description:
          "Returns sync status summary with counts by status, queue status, and a paginated list of matrices",
        tags: ["Sync"],
        querystring: SyncStatusQuerySchema,
        response: {
          200: SyncStatusResponseSchema,
        },
      },
    },
    async (request, reply) => {
      const { status, hasData, limit = 50, cursor } = request.query;

      // Get aggregate counts by sync_status
      const statusCounts = await db
        .selectFrom("matrices")
        .select(["sync_status", sql<number>`COUNT(*)::int`.as("count")])
        .groupBy("sync_status")
        .execute();

      // Get count of matrices with data
      const withDataResult = await db
        .selectFrom("matrices")
        .select(sql<number>`COUNT(DISTINCT matrices.id)::int`.as("count"))
        .innerJoin("statistics", "matrices.id", "statistics.matrix_id")
        .executeTakeFirst();

      const withDataCount = withDataResult?.count ?? 0;

      // Get queue status from sync_tasks
      const queueCounts = await db
        .selectFrom("sync_tasks")
        .select(["status", sql<number>`COUNT(*)::int`.as("count")])
        .where("status", "in", ["PENDING", "PLANNING", "RUNNING"])
        .groupBy("status")
        .execute();

      const queue = {
        pendingTasks: 0,
        planningTasks: 0,
        runningTasks: 0,
      };
      for (const row of queueCounts) {
        if (row.status === "PENDING") queue.pendingTasks = row.count;
        if (row.status === "PLANNING") queue.planningTasks = row.count;
        if (row.status === "RUNNING") queue.runningTasks = row.count;
      }

      // Build summary
      const summary = {
        total: 0,
        pending: 0,
        syncing: 0,
        synced: 0,
        failed: 0,
        stale: 0,
        withData: withDataCount,
      };

      for (const row of statusCounts) {
        const count = row.count;
        summary.total += count;
        switch (row.sync_status) {
          case "PENDING":
            summary.pending = count;
            break;
          case "SYNCING":
            summary.syncing = count;
            break;
          case "SYNCED":
            summary.synced = count;
            break;
          case "FAILED":
            summary.failed = count;
            break;
          case "STALE":
            summary.stale = count;
            break;
        }
      }

      // Build query for matrix list
      let query = db
        .selectFrom("matrices")
        .leftJoin(
          db
            .selectFrom("statistics")
            .select(["matrix_id", sql<number>`COUNT(*)::int`.as("data_count")])
            .groupBy("matrix_id")
            .as("stats"),
          "matrices.id",
          "stats.matrix_id"
        )
        .select([
          "matrices.id",
          "matrices.ins_code",
          "matrices.metadata",
          "matrices.sync_status",
          "matrices.last_sync_at",
          "matrices.sync_error",
          "stats.data_count",
        ])
        .orderBy("matrices.ins_code");

      // Apply filters
      if (status !== undefined) {
        query = query.where("matrices.sync_status", "=", status);
      }

      if (hasData === true) {
        query = query.where("stats.data_count", ">", 0);
      } else if (hasData === false) {
        query = query.where((eb) =>
          eb.or([
            eb("stats.data_count", "is", null),
            eb("stats.data_count", "=", 0),
          ])
        );
      }

      // Apply cursor pagination
      if (cursor !== undefined) {
        query = query.where("matrices.ins_code", ">", cursor);
      }

      // Fetch one extra to determine hasMore
      const rows = await query.limit(limit + 1).execute();

      const hasMore = rows.length > limit;
      const matrices = rows.slice(0, limit).map((row) => ({
        insCode: row.ins_code,
        name:
          (row.metadata as { names?: { ro?: string } })?.names?.ro ??
          row.ins_code,
        syncStatus: row.sync_status ?? "PENDING",
        lastSyncAt: row.last_sync_at?.toISOString() ?? null,
        hasData: (row.data_count ?? 0) > 0,
        dataPointCount: row.data_count ?? 0,
        syncError: row.sync_error ?? null,
      }));

      const lastItem = matrices[matrices.length - 1];
      const nextCursor = hasMore && lastItem ? lastItem.insCode : null;

      return reply.send({
        data: {
          summary,
          queue,
          matrices,
        },
        meta: {
          pagination: {
            cursor: nextCursor,
            hasMore,
            limit,
            total: summary.total,
          },
        },
      });
    }
  );

  // POST /sync/data/:matrixCode - Queue data sync task for a specific matrix
  app.post<{ Params: SyncDataParams; Body: SyncDataBody }>(
    "/sync/data/:matrixCode",
    {
      schema: {
        summary: "Queue data sync task",
        description:
          "Creates a sync task for a matrix. If a task is already pending or running for this matrix, returns the existing task. Matrix must have metadata synced first.",
        tags: ["Sync"],
        params: SyncDataParamsSchema,
        body: SyncDataBodySchema,
        response: {
          200: SyncTriggerResponseSchema,
          201: SyncTriggerResponseSchema,
        },
      },
    },
    async (request, reply) => {
      const { matrixCode } = request.params;
      const {
        yearFrom,
        yearTo,
        classificationMode,
        countyCode,
        priority = 0,
      } = request.body ?? {};

      // Apply defaults for year range
      const effectiveYearFrom = yearFrom ?? DEFAULT_YEAR_FROM;
      const effectiveYearTo = yearTo ?? DEFAULT_YEAR_TO;
      const effectiveMode = classificationMode ?? "totals-only";

      // Validate year range
      if (effectiveYearFrom > effectiveYearTo) {
        throw new ValidationError(
          "yearFrom must be less than or equal to yearTo",
          {
            yearFrom: effectiveYearFrom,
            yearTo: effectiveYearTo,
          }
        );
      }

      // Check if matrix exists and has metadata synced
      const matrix = await db
        .selectFrom("matrices")
        .select(["id", "ins_code", "sync_status", "metadata"])
        .where("ins_code", "=", matrixCode)
        .executeTakeFirst();

      if (!matrix) {
        throw new NotFoundError(`Matrix ${matrixCode} not found`);
      }

      if (matrix.sync_status !== "SYNCED") {
        throw new ValidationError(
          `Matrix ${matrixCode} metadata not synced (status: ${matrix.sync_status ?? "PENDING"}). Run 'pnpm cli sync matrices --code ${matrixCode}' first.`,
          { syncStatus: matrix.sync_status }
        );
      }

      // Check if partition exists
      const partitionName = `statistics_matrix_${String(matrix.id)}`;
      const partitionCheck = await sql<{ exists: boolean }>`
        SELECT EXISTS (
          SELECT 1 FROM pg_tables
          WHERE schemaname = 'public' AND tablename = ${partitionName}
        ) as exists
      `.execute(db);
      const partitionExists = partitionCheck.rows[0]?.exists ?? false;

      if (!partitionExists) {
        throw new ValidationError(
          `No partition exists for matrix ${matrixCode} (id: ${String(matrix.id)}). Run 'pnpm cli sync partitions --code ${matrixCode}' first.`,
          { matrixId: matrix.id }
        );
      }

      const matrixName =
        (matrix.metadata as { names?: { ro?: string } })?.names?.ro ??
        matrixCode;

      // Use SyncQueueService to create or get existing task
      const { task, isNew } = await queueService.createTask({
        matrixId: matrix.id,
        yearFrom: effectiveYearFrom,
        yearTo: effectiveYearTo,
        classificationMode: effectiveMode,
        countyCode,
        priority,
        createdBy: "api",
      });

      if (isNew) {
        return reply.status(201).send({
          data: {
            task: formatTask(task, matrixCode, matrixName),
            isNewTask: true,
            message: `Sync task created for matrix ${matrixCode} (years ${String(effectiveYearFrom)}-${String(effectiveYearTo)}). Task ID: ${String(task.id)}. Run 'pnpm cli sync worker' to process the queue.`,
          },
        });
      } else {
        return reply.status(200).send({
          data: {
            task: formatTask(task, matrixCode, matrixName),
            isNewTask: false,
            message: `Sync task already ${task.status.toLowerCase()} for matrix ${matrixCode}. Task ID: ${String(task.id)}`,
          },
        });
      }
    }
  );

  // GET /sync/tasks/:taskId - Get task status by ID
  app.get<{ Params: TaskIdParams }>(
    "/sync/tasks/:taskId",
    {
      schema: {
        summary: "Get sync task status",
        description: "Returns the status and details of a specific sync task",
        tags: ["Sync"],
        params: TaskIdParamsSchema,
        response: {
          200: SyncTaskResponseSchema,
        },
      },
    },
    async (request, reply) => {
      const taskId = Number.parseInt(request.params.taskId, 10);

      if (Number.isNaN(taskId)) {
        throw new ValidationError("Invalid task ID", {
          taskId: request.params.taskId,
        });
      }

      const taskWithProgress = await queueService.getTaskWithProgress(taskId);

      if (!taskWithProgress) {
        throw new NotFoundError(`Sync task ${String(taskId)} not found`);
      }

      return reply.send({
        data: formatTask(
          taskWithProgress,
          taskWithProgress.matrixCode,
          taskWithProgress.matrixName
        ),
      });
    }
  );

  // GET /sync/tasks - List sync tasks
  app.get<{ Querystring: SyncTasksQuery }>(
    "/sync/tasks",
    {
      schema: {
        summary: "List sync tasks",
        description:
          "Returns a paginated list of sync tasks with optional filters",
        tags: ["Sync"],
        querystring: SyncTasksQuerySchema,
        response: {
          200: SyncTasksListResponseSchema,
        },
      },
    },
    async (request, reply) => {
      const { status, matrixCode, limit = 20, cursor } = request.query;

      let query = db
        .selectFrom("sync_tasks")
        .innerJoin("matrices", "sync_tasks.matrix_id", "matrices.id")
        .select([
          "sync_tasks.id",
          "sync_tasks.matrix_id",
          "sync_tasks.status",
          "sync_tasks.year_from",
          "sync_tasks.year_to",
          "sync_tasks.classification_mode",
          "sync_tasks.county_code",
          "sync_tasks.priority",
          "sync_tasks.chunks_total",
          "sync_tasks.chunks_completed",
          "sync_tasks.chunks_failed",
          "sync_tasks.created_at",
          "sync_tasks.started_at",
          "sync_tasks.completed_at",
          "sync_tasks.rows_inserted",
          "sync_tasks.rows_updated",
          "sync_tasks.error_message",
          "sync_tasks.locked_until",
          "sync_tasks.locked_by",
          "sync_tasks.created_by",
          "matrices.ins_code",
          "matrices.metadata",
        ])
        .orderBy("sync_tasks.created_at", "desc");

      // Apply filters
      if (status !== undefined) {
        query = query.where("sync_tasks.status", "=", status as SyncTaskStatus);
      }

      if (matrixCode !== undefined) {
        query = query.where("matrices.ins_code", "=", matrixCode);
      }

      // Apply cursor pagination (cursor is task ID)
      if (cursor !== undefined) {
        const cursorId = Number.parseInt(cursor, 10);
        if (!Number.isNaN(cursorId)) {
          query = query.where("sync_tasks.id", "<", cursorId);
        }
      }

      // Fetch one extra to determine hasMore
      const rows = await query.limit(limit + 1).execute();

      const hasMore = rows.length > limit;
      const tasks = rows.slice(0, limit).map((row) => {
        const matrixName =
          (row.metadata as { names?: { ro?: string } })?.names?.ro ??
          row.ins_code;

        const task: SyncTask = {
          id: row.id,
          matrix_id: row.matrix_id,
          status: row.status,
          year_from: row.year_from,
          year_to: row.year_to,
          classification_mode: row.classification_mode,
          county_code: row.county_code,
          priority: row.priority,
          chunks_total: row.chunks_total,
          chunks_completed: row.chunks_completed,
          chunks_failed: row.chunks_failed,
          created_at: row.created_at,
          started_at: row.started_at,
          completed_at: row.completed_at,
          rows_inserted: row.rows_inserted,
          rows_updated: row.rows_updated,
          error_message: row.error_message,
          locked_until: row.locked_until,
          locked_by: row.locked_by,
          created_by: row.created_by,
        };

        return formatTask(task, row.ins_code, matrixName);
      });

      const lastTask = tasks[tasks.length - 1];
      const nextCursor = hasMore && lastTask ? String(lastTask.id) : null;

      // Get total count for this query (without pagination)
      let countQuery = db
        .selectFrom("sync_tasks")
        .select(sql<number>`COUNT(*)::int`.as("count"));

      if (status !== undefined) {
        countQuery = countQuery.where("status", "=", status as SyncTaskStatus);
      }

      if (matrixCode !== undefined) {
        countQuery = countQuery
          .innerJoin("matrices", "sync_tasks.matrix_id", "matrices.id")
          .where("matrices.ins_code", "=", matrixCode);
      }

      const countResult = await countQuery.executeTakeFirst();
      const total = countResult?.count ?? 0;

      return reply.send({
        data: tasks,
        meta: {
          pagination: {
            cursor: nextCursor,
            hasMore,
            limit,
            total,
          },
        },
      });
    }
  );

  // DELETE /sync/tasks/:taskId - Cancel a pending task
  app.delete<{ Params: TaskIdParams }>(
    "/sync/tasks/:taskId",
    {
      schema: {
        summary: "Cancel sync task",
        description:
          "Cancels a pending or planning sync task. Running tasks will be cancelled when they complete their current chunk.",
        tags: ["Sync"],
        params: TaskIdParamsSchema,
        response: {
          200: SyncTaskResponseSchema,
        },
      },
    },
    async (request, reply) => {
      const taskId = Number.parseInt(request.params.taskId, 10);

      if (Number.isNaN(taskId)) {
        throw new ValidationError("Invalid task ID", {
          taskId: request.params.taskId,
        });
      }

      // Get task with matrix info
      const taskWithProgress = await queueService.getTaskWithProgress(taskId);

      if (!taskWithProgress) {
        throw new NotFoundError(`Sync task ${String(taskId)} not found`);
      }

      if (
        !["PENDING", "PLANNING", "RUNNING"].includes(taskWithProgress.status)
      ) {
        throw new ValidationError(
          `Cannot cancel task with status ${taskWithProgress.status}. Only PENDING, PLANNING, or RUNNING tasks can be cancelled.`,
          { status: taskWithProgress.status }
        );
      }

      // Cancel the task
      const cancelled = await queueService.cancelTask(taskId);

      if (!cancelled) {
        throw new ValidationError(
          `Failed to cancel task ${String(taskId)}. It may have already completed.`,
          { taskId }
        );
      }

      // Get updated task
      const updatedTask = await queueService.getTaskWithProgress(taskId);

      if (!updatedTask) {
        throw new NotFoundError(`Sync task ${String(taskId)} not found`);
      }

      return reply.send({
        data: formatTask(
          updatedTask,
          updatedTask.matrixCode,
          updatedTask.matrixName
        ),
      });
    }
  );

  // POST /sync/tasks/:taskId/retry - Retry a failed task
  app.post<{ Params: TaskIdParams }>(
    "/sync/tasks/:taskId/retry",
    {
      schema: {
        summary: "Retry failed sync task",
        description: "Resets a failed sync task to pending status for retry",
        tags: ["Sync"],
        params: TaskIdParamsSchema,
        response: {
          200: TaskRetryResponseSchema,
        },
      },
    },
    async (request, reply) => {
      const taskId = Number.parseInt(request.params.taskId, 10);

      if (Number.isNaN(taskId)) {
        throw new ValidationError("Invalid task ID", {
          taskId: request.params.taskId,
        });
      }

      const success = await queueService.retryTask(taskId);

      if (success) {
        return reply.send({
          data: {
            success: true,
            message: `Task ${String(taskId)} reset for retry. Run 'pnpm cli sync worker' to process the queue.`,
          },
        });
      } else {
        return reply.send({
          data: {
            success: false,
            message: `Task ${String(taskId)} not found or not in FAILED status.`,
          },
        });
      }
    }
  );

  // GET /sync/system - Get sync system status
  app.get(
    "/sync/system",
    {
      schema: {
        summary: "Get sync system status",
        description:
          "Returns overall sync system status including task counts and rate limiter info",
        tags: ["Sync"],
        response: {
          200: Type.Object({
            data: Type.Object({
              tasks: Type.Object({
                pending: Type.Number(),
                planning: Type.Number(),
                running: Type.Number(),
                completed: Type.Number(),
                failed: Type.Number(),
                cancelled: Type.Number(),
              }),
              rateLimiter: Type.Object({
                isLocked: Type.Boolean(),
                lockedBy: Type.Union([Type.String(), Type.Null()]),
                lastCallAt: Type.Union([
                  Type.String({ format: "date-time" }),
                  Type.Null(),
                ]),
                callsToday: Type.Number(),
              }),
            }),
          }),
        },
      },
    },
    async (_request, reply) => {
      const status = await queueService.getSystemStatus();

      return reply.send({
        data: {
          tasks: status.tasks,
          rateLimiter: {
            ...status.rateLimiter,
            lastCallAt: status.rateLimiter.lastCallAt?.toISOString() ?? null,
          },
        },
      });
    }
  );
}
