/**
 * Sync API Routes
 *
 * Queue-based sync endpoints for monitoring sync status and triggering on-demand data sync.
 * Uses database as job queue to prevent INS API rate limiting and duplicate requests.
 */

import { Type, type Static } from "@sinclair/typebox";
import { sql } from "kysely";

import { db } from "../../db/connection.js";
import {
  SYNC_DEFAULTS,
  type SyncJobFlags,
  type SyncJobStatus,
} from "../../db/types.js";
import { NotFoundError, ValidationError } from "../plugins/error-handler.js";
import { MatrixStatusSchema, PaginationMetaSchema } from "../schemas/common.js";

import type { FastifyInstance } from "fastify";

// Re-export for use in this file
const DEFAULT_YEAR_FROM = SYNC_DEFAULTS.yearFrom;
const DEFAULT_YEAR_TO = SYNC_DEFAULTS.yearTo;
const DEFAULT_FLAGS: SyncJobFlags = SYNC_DEFAULTS.flags;

// ============================================================================
// Schemas
// ============================================================================

// Job status enum schema
const SyncJobStatusSchema = Type.Union([
  Type.Literal("PENDING"),
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
  pendingJobs: Type.Number(),
  runningJobs: Type.Number(),
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

// Sync job flags schema
const SyncJobFlagsSchema = Type.Object({
  skipExisting: Type.Optional(Type.Boolean()),
  force: Type.Optional(Type.Boolean()),
  chunkSize: Type.Optional(Type.Number({ minimum: 100, maximum: 30000 })),
  totalsOnly: Type.Optional(Type.Boolean()),
  includeAllClassifications: Type.Optional(Type.Boolean()),
});

// POST /sync/data/:matrixCode body
const SyncDataBodySchema = Type.Object({
  yearFrom: Type.Optional(Type.Number({ minimum: 1900, maximum: 2100 })),
  yearTo: Type.Optional(Type.Number({ minimum: 1900, maximum: 2100 })),
  priority: Type.Optional(
    Type.Number({ minimum: -10, maximum: 10, default: 0 })
  ),
  flags: Type.Optional(SyncJobFlagsSchema),
});

type SyncDataBody = Static<typeof SyncDataBodySchema>;

// Sync job schema for responses
const SyncJobSchema = Type.Object({
  id: Type.Number(),
  matrixCode: Type.String(),
  matrixName: Type.String(),
  status: SyncJobStatusSchema,
  yearFrom: Type.Number(),
  yearTo: Type.Number(),
  priority: Type.Number(),
  flags: SyncJobFlagsSchema,
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
    job: SyncJobSchema,
    isNewJob: Type.Boolean(),
    message: Type.String(),
  }),
});

// GET /sync/jobs/:jobId params
const JobIdParamsSchema = Type.Object({
  jobId: Type.String(),
});

type JobIdParams = Static<typeof JobIdParamsSchema>;

// GET /sync/jobs/:jobId response
const SyncJobResponseSchema = Type.Object({
  data: SyncJobSchema,
});

// GET /sync/jobs query params
const SyncJobsQuerySchema = Type.Object({
  status: Type.Optional(SyncJobStatusSchema),
  matrixCode: Type.Optional(Type.String()),
  limit: Type.Optional(Type.Number({ minimum: 1, maximum: 100, default: 20 })),
  cursor: Type.Optional(Type.String()),
});

type SyncJobsQuery = Static<typeof SyncJobsQuerySchema>;

// GET /sync/jobs response
const SyncJobsListResponseSchema = Type.Object({
  data: Type.Array(SyncJobSchema),
  meta: Type.Object({
    pagination: PaginationMetaSchema,
  }),
});

// ============================================================================
// Helper Functions
// ============================================================================

function formatJob(
  job: {
    id: number;
    matrix_id: number;
    status: SyncJobStatus;
    year_from: number | null;
    year_to: number | null;
    priority: number;
    flags: SyncJobFlags;
    created_at: Date;
    started_at: Date | null;
    completed_at: Date | null;
    rows_inserted: number;
    rows_updated: number;
    error_message: string | null;
  },
  matrixCode: string,
  matrixName: string
) {
  return {
    id: job.id,
    matrixCode,
    matrixName,
    status: job.status,
    yearFrom: job.year_from ?? DEFAULT_YEAR_FROM,
    yearTo: job.year_to ?? DEFAULT_YEAR_TO,
    priority: job.priority,
    flags: job.flags,
    createdAt: job.created_at.toISOString(),
    startedAt: job.started_at?.toISOString() ?? null,
    completedAt: job.completed_at?.toISOString() ?? null,
    rowsInserted: job.rows_inserted,
    rowsUpdated: job.rows_updated,
    errorMessage: job.error_message,
  };
}

// ============================================================================
// Route Registration
// ============================================================================

export function registerSyncRoutes(app: FastifyInstance): void {
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

      // Get queue status
      const queueCounts = await db
        .selectFrom("sync_jobs")
        .select(["status", sql<number>`COUNT(*)::int`.as("count")])
        .where("status", "in", ["PENDING", "RUNNING"])
        .groupBy("status")
        .execute();

      const queue = {
        pendingJobs: 0,
        runningJobs: 0,
      };
      for (const row of queueCounts) {
        if (row.status === "PENDING") queue.pendingJobs = row.count;
        if (row.status === "RUNNING") queue.runningJobs = row.count;
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

  // POST /sync/data/:matrixCode - Queue data sync job for a specific matrix
  app.post<{ Params: SyncDataParams; Body: SyncDataBody }>(
    "/sync/data/:matrixCode",
    {
      schema: {
        summary: "Queue data sync job",
        description:
          "Creates a sync job for a matrix. If a job is already pending or running for this matrix, returns the existing job. Matrix must have metadata synced first.",
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
      const { yearFrom, yearTo, priority = 0, flags } = request.body ?? {};

      // Apply defaults for year range
      const effectiveYearFrom = yearFrom ?? DEFAULT_YEAR_FROM;
      const effectiveYearTo = yearTo ?? DEFAULT_YEAR_TO;

      // Merge flags with defaults
      const effectiveFlags: SyncJobFlags = {
        ...DEFAULT_FLAGS,
        ...flags,
      };

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

      // Check for existing active job (PENDING or RUNNING)
      const existingJob = await db
        .selectFrom("sync_jobs")
        .selectAll()
        .where("matrix_id", "=", matrix.id)
        .where("status", "in", ["PENDING", "RUNNING"])
        .executeTakeFirst();

      if (existingJob) {
        // Return existing job - no duplicate
        return reply.status(200).send({
          data: {
            job: formatJob(existingJob, matrixCode, matrixName),
            isNewJob: false,
            message: `Sync job already ${existingJob.status.toLowerCase()} for matrix ${matrixCode}. Job ID: ${String(existingJob.id)}`,
          },
        });
      }

      // Create new job with defaults applied
      const newJob = await db
        .insertInto("sync_jobs")
        .values({
          matrix_id: matrix.id,
          status: "PENDING",
          year_from: effectiveYearFrom,
          year_to: effectiveYearTo,
          priority,
          flags: effectiveFlags,
          rows_inserted: 0,
          rows_updated: 0,
          created_by: "api",
        })
        .returningAll()
        .executeTakeFirstOrThrow();

      return reply.status(201).send({
        data: {
          job: formatJob(newJob, matrixCode, matrixName),
          isNewJob: true,
          message: `Sync job created for matrix ${matrixCode} (years ${String(effectiveYearFrom)}-${String(effectiveYearTo)}). Job ID: ${String(newJob.id)}. Run 'pnpm cli sync worker' to process the queue.`,
        },
      });
    }
  );

  // GET /sync/jobs/:jobId - Get job status by ID
  app.get<{ Params: JobIdParams }>(
    "/sync/jobs/:jobId",
    {
      schema: {
        summary: "Get sync job status",
        description: "Returns the status and details of a specific sync job",
        tags: ["Sync"],
        params: JobIdParamsSchema,
        response: {
          200: SyncJobResponseSchema,
        },
      },
    },
    async (request, reply) => {
      const jobId = Number.parseInt(request.params.jobId, 10);

      if (Number.isNaN(jobId)) {
        throw new ValidationError("Invalid job ID", {
          jobId: request.params.jobId,
        });
      }

      const job = await db
        .selectFrom("sync_jobs")
        .innerJoin("matrices", "sync_jobs.matrix_id", "matrices.id")
        .select([
          "sync_jobs.id",
          "sync_jobs.matrix_id",
          "sync_jobs.status",
          "sync_jobs.year_from",
          "sync_jobs.year_to",
          "sync_jobs.priority",
          "sync_jobs.flags",
          "sync_jobs.created_at",
          "sync_jobs.started_at",
          "sync_jobs.completed_at",
          "sync_jobs.rows_inserted",
          "sync_jobs.rows_updated",
          "sync_jobs.error_message",
          "matrices.ins_code",
          "matrices.metadata",
        ])
        .where("sync_jobs.id", "=", jobId)
        .executeTakeFirst();

      if (!job) {
        throw new NotFoundError(`Sync job ${String(jobId)} not found`);
      }

      const matrixName =
        (job.metadata as { names?: { ro?: string } })?.names?.ro ??
        job.ins_code;

      return reply.send({
        data: formatJob(job, job.ins_code, matrixName),
      });
    }
  );

  // GET /sync/jobs - List sync jobs
  app.get<{ Querystring: SyncJobsQuery }>(
    "/sync/jobs",
    {
      schema: {
        summary: "List sync jobs",
        description:
          "Returns a paginated list of sync jobs with optional filters",
        tags: ["Sync"],
        querystring: SyncJobsQuerySchema,
        response: {
          200: SyncJobsListResponseSchema,
        },
      },
    },
    async (request, reply) => {
      const { status, matrixCode, limit = 20, cursor } = request.query;

      let query = db
        .selectFrom("sync_jobs")
        .innerJoin("matrices", "sync_jobs.matrix_id", "matrices.id")
        .select([
          "sync_jobs.id",
          "sync_jobs.matrix_id",
          "sync_jobs.status",
          "sync_jobs.year_from",
          "sync_jobs.year_to",
          "sync_jobs.priority",
          "sync_jobs.flags",
          "sync_jobs.created_at",
          "sync_jobs.started_at",
          "sync_jobs.completed_at",
          "sync_jobs.rows_inserted",
          "sync_jobs.rows_updated",
          "sync_jobs.error_message",
          "matrices.ins_code",
          "matrices.metadata",
        ])
        .orderBy("sync_jobs.created_at", "desc");

      // Apply filters
      if (status !== undefined) {
        query = query.where("sync_jobs.status", "=", status);
      }

      if (matrixCode !== undefined) {
        query = query.where("matrices.ins_code", "=", matrixCode);
      }

      // Apply cursor pagination (cursor is job ID)
      if (cursor !== undefined) {
        const cursorId = Number.parseInt(cursor, 10);
        if (!Number.isNaN(cursorId)) {
          query = query.where("sync_jobs.id", "<", cursorId);
        }
      }

      // Fetch one extra to determine hasMore
      const rows = await query.limit(limit + 1).execute();

      const hasMore = rows.length > limit;
      const jobs = rows.slice(0, limit).map((job) => {
        const matrixName =
          (job.metadata as { names?: { ro?: string } })?.names?.ro ??
          job.ins_code;
        return formatJob(job, job.ins_code, matrixName);
      });

      const lastJob = jobs[jobs.length - 1];
      const nextCursor = hasMore && lastJob ? String(lastJob.id) : null;

      // Get total count for this query (without pagination)
      let countQuery = db
        .selectFrom("sync_jobs")
        .select(sql<number>`COUNT(*)::int`.as("count"));

      if (status !== undefined) {
        countQuery = countQuery.where("status", "=", status);
      }

      if (matrixCode !== undefined) {
        countQuery = countQuery
          .innerJoin("matrices", "sync_jobs.matrix_id", "matrices.id")
          .where("matrices.ins_code", "=", matrixCode);
      }

      const countResult = await countQuery.executeTakeFirst();
      const total = countResult?.count ?? 0;

      return reply.send({
        data: jobs,
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

  // DELETE /sync/jobs/:jobId - Cancel a pending job
  app.delete<{ Params: JobIdParams }>(
    "/sync/jobs/:jobId",
    {
      schema: {
        summary: "Cancel sync job",
        description:
          "Cancels a pending sync job. Running jobs cannot be cancelled.",
        tags: ["Sync"],
        params: JobIdParamsSchema,
        response: {
          200: SyncJobResponseSchema,
        },
      },
    },
    async (request, reply) => {
      const jobId = Number.parseInt(request.params.jobId, 10);

      if (Number.isNaN(jobId)) {
        throw new ValidationError("Invalid job ID", {
          jobId: request.params.jobId,
        });
      }

      // Get job with matrix info
      const job = await db
        .selectFrom("sync_jobs")
        .innerJoin("matrices", "sync_jobs.matrix_id", "matrices.id")
        .select([
          "sync_jobs.id",
          "sync_jobs.status",
          "matrices.ins_code",
          "matrices.metadata",
        ])
        .where("sync_jobs.id", "=", jobId)
        .executeTakeFirst();

      if (!job) {
        throw new NotFoundError(`Sync job ${String(jobId)} not found`);
      }

      if (job.status !== "PENDING") {
        throw new ValidationError(
          `Cannot cancel job with status ${job.status}. Only PENDING jobs can be cancelled.`,
          { status: job.status }
        );
      }

      // Cancel the job
      const updatedJob = await db
        .updateTable("sync_jobs")
        .set({
          status: "CANCELLED",
          completed_at: new Date(),
        })
        .where("id", "=", jobId)
        .returningAll()
        .executeTakeFirstOrThrow();

      const matrixName =
        (job.metadata as { names?: { ro?: string } })?.names?.ro ??
        job.ins_code;

      return reply.send({
        data: formatJob(updatedJob, job.ins_code, matrixName),
      });
    }
  );
}
