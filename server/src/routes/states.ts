/**
 * State inspection routes
 */

import { Router } from 'express';
import type { JsStore, StateResponse } from '../types.js';

export function createStateRoutes(store: JsStore): Router {
  const router = Router();

  // Check if a state ID is registered (exists but may be empty)
  function isRegistered(stateId: string): boolean {
    return store.listStates().some(s => s.id === stateId);
  }

  // GET / - List all states
  router.get('/', (_req, res) => {
    try {
      const states = store.listStates();
      res.json({
        success: true,
        data: states.map(s => ({
          id: s.id,
          strategy: s.strategy,
          itemCount: s.itemCount ?? undefined,
          opsSinceSnapshot: s.opsSinceSnapshot,
        })),
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
    }
  });

  // GET /:id - Get state by ID (current value)
  router.get('/:id', (req, res) => {
    try {
      const stateId = decodeURIComponent(req.params.id);

      // Find state info first
      const states = store.listStates();
      const stateInfo = states.find(s => s.id === stateId);

      if (!stateInfo) {
        res.status(404).json({
          success: false,
          error: 'State not found',
        });
        return;
      }

      // For append_log states, don't fetch full value (use tail/slice instead)
      // This avoids loading thousands of items just to display state info
      const isAppendLog = stateInfo.strategy?.toLowerCase() === 'append_log' ||
                          stateInfo.strategy?.toLowerCase() === 'appendlog';

      let value: unknown = null;
      if (!isAppendLog) {
        value = store.getStateJson(stateId);
      }

      const response: StateResponse = {
        id: stateId,
        strategy: stateInfo.strategy ?? 'unknown',
        itemCount: stateInfo.itemCount ?? undefined,
        opsSinceSnapshot: stateInfo.opsSinceSnapshot ?? 0,
        value,
      };

      res.json({
        success: true,
        data: response,
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
    }
  });

  // GET /:id/at/:sequence - Get state at specific sequence
  router.get('/:id/at/:sequence', (req, res) => {
    try {
      const stateId = decodeURIComponent(req.params.id);
      const sequence = parseInt(req.params.sequence, 10);

      if (isNaN(sequence) || sequence < 0) {
        res.status(400).json({
          success: false,
          error: 'Invalid sequence number',
        });
        return;
      }

      const value = store.getStateJsonAt(stateId, sequence);

      if (value === null) {
        res.status(404).json({
          success: false,
          error: 'State not found at that sequence',
        });
        return;
      }

      res.json({
        success: true,
        data: {
          id: stateId,
          sequence,
          value,
        },
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
    }
  });

  // GET /:id/slice - Get slice of append-log state
  router.get('/:id/slice', (req, res) => {
    try {
      const stateId = decodeURIComponent(req.params.id);
      const offset = req.query.offset ? parseInt(req.query.offset as string, 10) : 0;
      const limit = req.query.limit ? parseInt(req.query.limit as string, 10) : 100;

      const sliceBuffer = store.getStateSlice(stateId, offset, limit);

      if (sliceBuffer === null) {
        // Registered but empty → return empty results
        if (isRegistered(stateId)) {
          res.json({
            success: true,
            data: { items: [], offset, limit, total: 0, hasMore: false },
          });
          return;
        }
        res.status(404).json({
          success: false,
          error: 'State not found or not an append-log state',
        });
        return;
      }

      // Parse slice as JSON array
      let items: unknown[];
      try {
        items = JSON.parse(sliceBuffer.toString('utf8'));
      } catch {
        items = [];
      }

      const totalLen = store.getStateLen(stateId);

      res.json({
        success: true,
        data: {
          items,
          offset,
          limit,
          total: totalLen,
          hasMore: totalLen !== null && offset + items.length < totalLen,
        },
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
    }
  });

  // GET /:id/tail - Get last N items from append-log state
  router.get('/:id/tail', (req, res) => {
    try {
      const stateId = decodeURIComponent(req.params.id);
      const count = req.query.count ? parseInt(req.query.count as string, 10) : 10;

      const tailBuffer = store.getStateTail(stateId, count);

      if (tailBuffer === null) {
        if (isRegistered(stateId)) {
          res.json({
            success: true,
            data: { items: [], count, total: 0 },
          });
          return;
        }
        res.status(404).json({
          success: false,
          error: 'State not found or not an append-log state',
        });
        return;
      }

      // Parse tail as JSON array
      let items: unknown[];
      try {
        items = JSON.parse(tailBuffer.toString('utf8'));
      } catch {
        items = [];
      }

      const totalLen = store.getStateLen(stateId);

      res.json({
        success: true,
        data: {
          items,
          count,
          total: totalLen,
        },
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
    }
  });

  // GET /:id/length - Get length of append-log state
  router.get('/:id/length', (req, res) => {
    try {
      const stateId = decodeURIComponent(req.params.id);
      const length = store.getStateLen(stateId);

      if (length === null) {
        if (isRegistered(stateId)) {
          res.json({
            success: true,
            data: { length: 0 },
          });
          return;
        }
        res.status(404).json({
          success: false,
          error: 'State not found or not an append-log state',
        });
        return;
      }

      res.json({
        success: true,
        data: { length },
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
    }
  });

  // GET /:id/search - Search through append-log state items
  router.get('/:id/search', (req, res) => {
    try {
      const stateId = decodeURIComponent(req.params.id);
      const query = (req.query.q as string) || '';
      const limit = req.query.limit ? parseInt(req.query.limit as string, 10) : 100;
      const field = req.query.field as string | undefined; // Optional: search specific field

      if (!query) {
        res.status(400).json({
          success: false,
          error: 'Search query (q) is required',
        });
        return;
      }

      const totalLen = store.getStateLen(stateId);
      if (totalLen === null) {
        if (isRegistered(stateId)) {
          res.json({
            success: true,
            data: { items: [], total: 0, query, field: field || null },
          });
          return;
        }
        res.status(404).json({
          success: false,
          error: 'State not found or not an append-log state',
        });
        return;
      }

      // Fetch all items and search (for small datasets; large datasets would need index)
      const allBuffer = store.getStateSlice(stateId, 0, totalLen);
      if (allBuffer === null) {
        res.json({
          success: true,
          data: { items: [], total: 0, query },
        });
        return;
      }

      let allItems: unknown[];
      try {
        allItems = JSON.parse(allBuffer.toString('utf8'));
      } catch {
        allItems = [];
      }

      // Search through items
      const queryLower = query.toLowerCase();
      const matches: { index: number; item: unknown }[] = [];

      for (let i = 0; i < allItems.length && matches.length < limit; i++) {
        const item = allItems[i];
        let match = false;

        if (field && typeof item === 'object' && item !== null) {
          // Search specific field
          const fieldValue = (item as Record<string, unknown>)[field];
          if (fieldValue !== undefined) {
            match = String(fieldValue).toLowerCase().includes(queryLower);
          }
        } else {
          // Search entire item as JSON string
          match = JSON.stringify(item).toLowerCase().includes(queryLower);
        }

        if (match) {
          matches.push({ index: i, item });
        }
      }

      res.json({
        success: true,
        data: {
          items: matches,
          total: matches.length,
          query,
          field: field || null,
        },
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
    }
  });

  return router;
}
