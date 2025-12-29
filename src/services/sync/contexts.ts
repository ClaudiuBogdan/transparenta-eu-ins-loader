import { logger } from "../../logger.js";
import { fetchContexts } from "../../scraper/client.js";

import type { Database, NewContext, SyncResult } from "../../db/types.js";
import type { InsContext } from "../../types/index.js";
import type { Kysely } from "kysely";

// ============================================================================
// Context Sync Service
// ============================================================================

export class ContextSyncService {
  constructor(private db: Kysely<Database>) {}

  /**
   * Sync all contexts from INS API
   */
  async sync(): Promise<SyncResult> {
    const startTime = Date.now();
    logger.info("Starting context sync");

    // Fetch all contexts from API
    const contexts = await fetchContexts();
    logger.info({ count: contexts.length }, "Fetched contexts from API");

    // Sort by level (parents first)
    const sorted = this.topologicalSort(contexts);

    let inserted = 0;
    let updated = 0;

    // Build code -> id mapping as we insert
    const codeToId = new Map<string, number>();

    for (const ctx of sorted) {
      // Find parent ID
      const parentId =
        ctx.parentCode !== "0" ? (codeToId.get(ctx.parentCode) ?? null) : null;

      // Check if exists
      const existing = await this.db
        .selectFrom("contexts")
        .select("id")
        .where("ins_code", "=", ctx.context.code)
        .executeTakeFirst();

      if (existing) {
        // Update
        await this.db
          .updateTable("contexts")
          .set({
            name: ctx.context.name,
            level: ctx.level,
            parent_id: parentId,
            children_type: ctx.context.childrenUrl,
            updated_at: new Date(),
          })
          .where("id", "=", existing.id)
          .execute();
        codeToId.set(ctx.context.code, existing.id);
        updated++;
      } else {
        // Insert
        const newContext: NewContext = {
          ins_code: ctx.context.code,
          name: ctx.context.name,
          level: ctx.level,
          parent_id: parentId,
          children_type: ctx.context.childrenUrl,
          path: "", // Will be computed by trigger
        };

        const result = await this.db
          .insertInto("contexts")
          .values(newContext)
          .returning("id")
          .executeTakeFirst();

        if (result) {
          codeToId.set(ctx.context.code, result.id);
          inserted++;
        }
      }
    }

    const duration = Date.now() - startTime;
    logger.info({ inserted, updated, duration }, "Context sync completed");

    return { inserted, updated, duration };
  }

  /**
   * Get context ID by INS code
   */
  async getContextIdByCode(code: string): Promise<number | null> {
    const result = await this.db
      .selectFrom("contexts")
      .select("id")
      .where("ins_code", "=", code)
      .executeTakeFirst();
    return result?.id ?? null;
  }

  /**
   * Get all contexts as a tree
   */
  async getContextTree(): Promise<ContextTreeNode[]> {
    const all = await this.db
      .selectFrom("contexts")
      .selectAll()
      .orderBy("level")
      .orderBy("name")
      .execute();

    // Build tree
    const byId = new Map<number, ContextTreeNode>();
    const roots: ContextTreeNode[] = [];

    for (const ctx of all) {
      const node: ContextTreeNode = {
        id: ctx.id,
        code: ctx.ins_code,
        name: ctx.name,
        level: ctx.level,
        childrenType: ctx.children_type,
        children: [],
      };
      byId.set(ctx.id, node);

      if (ctx.parent_id === null) {
        roots.push(node);
      } else {
        const parent = byId.get(ctx.parent_id);
        if (parent) {
          parent.children.push(node);
        }
      }
    }

    return roots;
  }

  /**
   * Sort contexts topologically (parents before children)
   */
  private topologicalSort(contexts: InsContext[]): InsContext[] {
    return [...contexts].sort((a, b) => {
      // Sort by level first
      if (a.level !== b.level) {
        return a.level - b.level;
      }
      // Then by code
      return a.context.code.localeCompare(b.context.code);
    });
  }
}

// ============================================================================
// Types
// ============================================================================

interface ContextTreeNode {
  id: number;
  code: string;
  name: string;
  level: number;
  childrenType: "context" | "matrix";
  children: ContextTreeNode[];
}
