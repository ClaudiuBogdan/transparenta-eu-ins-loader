/**
 * Tags Routes - /api/v1/tags
 * Matrix tags for discoverability and categorization
 */

import { Type, type Static } from "@sinclair/typebox";
import { sql } from "kysely";

import { db } from "../../db/connection.js";
import { NotFoundError } from "../plugins/error-handler.js";
import { LocaleSchema, type Locale } from "../schemas/common.js";

import type { FastifyInstance } from "fastify";

// ============================================================================
// Schemas
// ============================================================================

const ListTagsQuerySchema = Type.Object({
  locale: Type.Optional(LocaleSchema),
  category: Type.Optional(
    Type.String({
      description: "Filter by category (topic, audience, use-case)",
    })
  ),
  withMatrixCounts: Type.Optional(
    Type.Boolean({ description: "Include matrix counts", default: true })
  ),
});

type ListTagsQuery = Static<typeof ListTagsQuerySchema>;

const TagSlugParamSchema = Type.Object({
  slug: Type.String({ description: "Tag slug" }),
});

const TagMatricesQuerySchema = Type.Object({
  locale: Type.Optional(LocaleSchema),
  limit: Type.Optional(
    Type.Number({ description: "Limit results", default: 50 })
  ),
});

type TagMatricesQuery = Static<typeof TagMatricesQuerySchema>;

// ============================================================================
// Helper Functions
// ============================================================================

function getLocalizedName(
  name: string,
  nameEn: string | null,
  locale: Locale
): string {
  return locale === "en" && nameEn ? nameEn : name;
}

// ============================================================================
// Routes
// ============================================================================

export function registerTagRoutes(app: FastifyInstance): void {
  /**
   * GET /api/v1/tags
   * List all tags with matrix counts
   */
  app.get<{ Querystring: ListTagsQuery }>(
    "/tags",
    {
      schema: {
        summary: "List all tags",
        description:
          "Get all matrix tags grouped by category with optional matrix counts.",
        tags: ["Tags"],
        querystring: ListTagsQuerySchema,
      },
    },
    async (request) => {
      const {
        locale = "ro",
        category,
        withMatrixCounts = true,
      } = request.query;

      let query = db
        .selectFrom("matrix_tags")
        .select([
          "matrix_tags.id",
          "matrix_tags.name",
          "matrix_tags.name_en",
          "matrix_tags.slug",
          "matrix_tags.category",
          "matrix_tags.description",
          "matrix_tags.usage_count",
        ]);

      if (category) {
        query = query.where("matrix_tags.category", "=", category);
      }

      const tags = await query
        .orderBy("matrix_tags.category")
        .orderBy("matrix_tags.name")
        .execute();

      // Get matrix counts if requested
      let matrixCounts = new Map<number, number>();

      if (withMatrixCounts) {
        const counts = await db
          .selectFrom("matrix_tag_assignments")
          .select(["tag_id", sql<number>`COUNT(*)`.as("count")])
          .groupBy("tag_id")
          .execute();

        matrixCounts = new Map(counts.map((c) => [c.tag_id, c.count]));
      }

      const result = tags.map((t) => ({
        id: t.id,
        name: getLocalizedName(t.name, t.name_en, locale),
        slug: t.slug,
        category: t.category,
        description: t.description,
        matrixCount: withMatrixCounts ? (matrixCounts.get(t.id) ?? 0) : null,
      }));

      // Group by category
      const byCategory: Record<string, typeof result> = {};
      for (const tag of result) {
        byCategory[tag.category] ??= [];
        byCategory[tag.category]!.push(tag);
      }

      return {
        data: {
          tags: result,
          byCategory,
          total: result.length,
        },
      };
    }
  );

  /**
   * GET /api/v1/tags/:slug
   * Get tag details with associated matrices
   */
  app.get<{
    Params: Static<typeof TagSlugParamSchema>;
    Querystring: TagMatricesQuery;
  }>(
    "/tags/:slug",
    {
      schema: {
        summary: "Get tag with matrices",
        description:
          "Get tag details and all matrices associated with this tag. " +
          "Example: `/tags/population`",
        tags: ["Tags"],
        params: TagSlugParamSchema,
        querystring: TagMatricesQuerySchema,
      },
    },
    async (request) => {
      const { slug } = request.params;
      const { locale = "ro", limit = 50 } = request.query;

      // Get tag
      const tag = await db
        .selectFrom("matrix_tags")
        .select(["id", "name", "name_en", "slug", "category", "description"])
        .where("slug", "=", slug)
        .executeTakeFirst();

      if (!tag) {
        throw new NotFoundError(`Tag with slug ${slug} not found`);
      }

      // Get associated matrices
      const matrices = await db
        .selectFrom("matrix_tag_assignments")
        .innerJoin(
          "matrices",
          "matrix_tag_assignments.matrix_id",
          "matrices.id"
        )
        .leftJoin("contexts", "matrices.context_id", "contexts.id")
        .select([
          "matrices.id",
          "matrices.ins_code",
          "matrices.metadata",
          "matrices.dimensions",
          "contexts.names as context_names",
        ])
        .where("matrix_tag_assignments.tag_id", "=", tag.id)
        .limit(limit)
        .execute();

      const matrixDtos = matrices.map((m) => {
        const metadata = m.metadata;
        const contextNames = m.context_names;

        return {
          id: m.id,
          insCode: m.ins_code,
          name:
            locale === "en" && metadata.names.en
              ? metadata.names.en
              : metadata.names.ro,
          contextName: contextNames
            ? locale === "en" && contextNames.en
              ? contextNames.en
              : contextNames.ro
            : null,
          periodicity: metadata.periodicity ?? [],
          dimensionCount: (m.dimensions ?? []).length,
          startYear: metadata.yearRange?.[0] ?? null,
          endYear: metadata.yearRange?.[1] ?? null,
        };
      });

      return {
        data: {
          tag: {
            id: tag.id,
            name: getLocalizedName(tag.name, tag.name_en, locale),
            slug: tag.slug,
            category: tag.category,
            description: tag.description,
          },
          matrices: matrixDtos,
          matrixCount: matrixDtos.length,
        },
      };
    }
  );

  /**
   * GET /api/v1/tags/:slug/related
   * Get related tags (tags that share matrices with this tag)
   */
  app.get<{
    Params: Static<typeof TagSlugParamSchema>;
    Querystring: { locale?: Locale };
  }>(
    "/tags/:slug/related",
    {
      schema: {
        summary: "Get related tags",
        description:
          "Get tags that are commonly used together with this tag (share matrices).",
        tags: ["Tags"],
        params: TagSlugParamSchema,
        querystring: Type.Object({
          locale: Type.Optional(LocaleSchema),
        }),
      },
    },
    async (request) => {
      const { slug } = request.params;
      const locale = request.query.locale ?? "ro";

      // Get tag
      const tag = await db
        .selectFrom("matrix_tags")
        .select(["id", "name", "name_en", "slug"])
        .where("slug", "=", slug)
        .executeTakeFirst();

      if (!tag) {
        throw new NotFoundError(`Tag with slug ${slug} not found`);
      }

      // Find related tags through matrix co-occurrence
      const relatedTags = await db
        .selectFrom("matrix_tag_assignments as mta1")
        .innerJoin(
          "matrix_tag_assignments as mta2",
          "mta1.matrix_id",
          "mta2.matrix_id"
        )
        .innerJoin("matrix_tags", "mta2.tag_id", "matrix_tags.id")
        .select([
          "matrix_tags.id",
          "matrix_tags.name",
          "matrix_tags.name_en",
          "matrix_tags.slug",
          "matrix_tags.category",
          sql<number>`COUNT(DISTINCT mta1.matrix_id)`.as("shared_count"),
        ])
        .where("mta1.tag_id", "=", tag.id)
        .where("mta2.tag_id", "!=", tag.id)
        .groupBy([
          "matrix_tags.id",
          "matrix_tags.name",
          "matrix_tags.name_en",
          "matrix_tags.slug",
          "matrix_tags.category",
        ])
        .orderBy(sql`COUNT(DISTINCT mta1.matrix_id)`, "desc")
        .limit(10)
        .execute();

      return {
        data: {
          tag: {
            id: tag.id,
            name: getLocalizedName(tag.name, tag.name_en, locale),
            slug: tag.slug,
          },
          relatedTags: relatedTags.map((t) => ({
            id: t.id,
            name: getLocalizedName(t.name, t.name_en, locale),
            slug: t.slug,
            category: t.category,
            sharedMatrixCount: t.shared_count,
          })),
        },
      };
    }
  );
}
