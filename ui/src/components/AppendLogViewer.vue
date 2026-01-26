<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted, watch, nextTick } from 'vue';
import { client } from '@/api/client';
import { useChronicleWebSocket } from '@/api/websocket';
import JsonViewer from '@/components/JsonViewer.vue';

const props = defineProps<{
  stateId: string;
}>();

type LogItem = {
  timestamp?: number;
  type?: string;
  source?: string;
  message?: string;
  [key: string]: unknown;
};

type ViewMode = 'log' | 'search';

const viewMode = ref<ViewMode>('log');
const items = ref<LogItem[]>([]);
const searchQuery = ref('');
const searchResults = ref<{ index: number; item: LogItem }[]>([]);
const searchLoading = ref(false);
const loading = ref(false);
const loadingMore = ref(false);
const error = ref<string | null>(null);
const totalCount = ref(0);
const isSubscribed = ref(false);

// Track how much history we've loaded (index from end, since we show newest first)
const loadedCount = ref(0);
const batchSize = 50;

const ws = useChronicleWebSocket();
const scrollContainer = ref<HTMLElement | null>(null);
const sentinel = ref<HTMLElement | null>(null);
let observer: IntersectionObserver | null = null;

// Track expanded items (by index)
const expandedItems = ref<Set<number>>(new Set());

function toggleExpand(idx: number) {
  if (expandedItems.value.has(idx)) {
    expandedItems.value.delete(idx);
  } else {
    expandedItems.value.add(idx);
  }
  // Trigger reactivity
  expandedItems.value = new Set(expandedItems.value);
}

function isExpanded(idx: number): boolean {
  return expandedItems.value.has(idx);
}

// Format timestamp - show date if not today
function formatTime(ts?: number): string {
  if (!ts) return '';
  const date = new Date(ts);
  const today = new Date();
  const isToday = date.getFullYear() === today.getFullYear() &&
                  date.getMonth() === today.getMonth() &&
                  date.getDate() === today.getDate();
  return isToday ? date.toLocaleTimeString() : date.toLocaleString();
}

// Get log type color
function getTypeColor(type?: string): string {
  switch (type?.toUpperCase()) {
    case 'ERROR': return 'text-red-600 bg-red-50';
    case 'WARN': return 'text-yellow-600 bg-yellow-50';
    case 'DEBUG': return 'text-gray-500 bg-gray-50';
    case 'INFO':
    default: return 'text-blue-600 bg-blue-50';
  }
}

// Check if we can load more history
const hasMore = computed(() => loadedCount.value < totalCount.value);

// Initial load - fetch recent items
async function fetchInitial() {
  loading.value = true;
  error.value = null;
  try {
    const result = await client.getStateTail(props.stateId, batchSize);
    // Reverse to show newest first
    items.value = (result.items as LogItem[]).reverse();
    totalCount.value = result.total ?? 0;
    loadedCount.value = items.value.length;
  } catch (e) {
    error.value = e instanceof Error ? e.message : 'Failed to fetch logs';
  } finally {
    loading.value = false;
  }
}

// Load more history when scrolling down
async function loadMore() {
  if (loadingMore.value || !hasMore.value) return;

  loadingMore.value = true;
  try {
    // Calculate offset from start (oldest items)
    // We want items older than what we have
    // Total items: totalCount, we have loadedCount newest ones
    // So we need items from index (totalCount - loadedCount - batchSize) to (totalCount - loadedCount - 1)
    const endIndex = totalCount.value - loadedCount.value;
    const startIndex = Math.max(0, endIndex - batchSize);
    const limit = endIndex - startIndex;

    if (limit <= 0) return;

    const result = await client.getStateSlice(props.stateId, startIndex, limit);
    const olderItems = (result.items as LogItem[]).reverse(); // Reverse to get newest-of-older first

    // Append to end (older items go below)
    items.value = [...items.value, ...olderItems];
    loadedCount.value += olderItems.length;
  } catch (e) {
    console.error('Failed to load more:', e);
  } finally {
    loadingMore.value = false;
  }
}

// Search logs
async function doSearch() {
  if (!searchQuery.value.trim()) {
    searchResults.value = [];
    return;
  }
  searchLoading.value = true;
  error.value = null;
  try {
    const result = await client.searchState(props.stateId, searchQuery.value, { limit: 100 });
    searchResults.value = (result.items as { index: number; item: LogItem }[]).reverse();
  } catch (e) {
    error.value = e instanceof Error ? e.message : 'Search failed';
  } finally {
    searchLoading.value = false;
  }
}

let unregisterHandler: (() => void) | null = null;

// Handle incoming WebSocket events
function handleWsEvent(event: { eventType: string; data: unknown }) {
  // Handle state delta events (for append_log changes)
  if (event.eventType === 'state_delta') {
    const data = event.data as {
      type?: string;
      state_id?: string;
      operation?: { Append?: unknown };
      sequence?: number;
    };

    if (data.state_id === props.stateId && data.operation?.Append) {
      // Append operation contains raw bytes - decode UTF-8 and parse JSON
      let appendedItem: LogItem;
      const appendData = data.operation.Append;

      if (Array.isArray(appendData)) {
        // Byte array - decode to string then parse JSON
        const bytes = new Uint8Array(appendData as number[]);
        const jsonStr = new TextDecoder().decode(bytes);
        appendedItem = JSON.parse(jsonStr);
      } else {
        // Already parsed object
        appendedItem = appendData as LogItem;
      }

      // Prepend to show latest first (new items at top)
      items.value = [appendedItem, ...items.value];
      totalCount.value++;
      loadedCount.value++;
    }
  }

  // Handle state snapshot events (initial state or full refresh)
  if (event.eventType === 'state_snapshot') {
    const data = event.data as {
      type?: string;
      state_id?: string;
      data?: unknown;
    };

    if (data.state_id === props.stateId) {
      fetchInitial();
    }
  }
}

// Subscribe to state updates
function subscribe() {
  if (isSubscribed.value) return;

  ws.connect();
  unregisterHandler = ws.onEvent(handleWsEvent);
  ws.subscribe({
    filter: {
      stateIds: [props.stateId],
      includeStateChanges: true,
      includeRecords: true,
    },
  });
  isSubscribed.value = true;
}

// Unsubscribe
function unsubscribe() {
  if (!isSubscribed.value) return;

  if (unregisterHandler) {
    unregisterHandler();
    unregisterHandler = null;
  }

  ws.unsubscribe();
  isSubscribed.value = false;
}

// Setup intersection observer for infinite scroll
function setupObserver() {
  if (observer) observer.disconnect();

  observer = new IntersectionObserver(
    (entries) => {
      if (entries[0]?.isIntersecting && hasMore.value && !loadingMore.value) {
        loadMore();
      }
    },
    { root: scrollContainer.value, threshold: 0.1 }
  );

  if (sentinel.value) {
    observer.observe(sentinel.value);
  }
}

// Watch for WebSocket reconnection
watch(ws.status, (newStatus, oldStatus) => {
  if (newStatus === 'connected' && oldStatus === 'disconnected') {
    console.log('[AppendLogViewer] WebSocket reconnected, re-subscribing');
    isSubscribed.value = false;
    subscribe();
  }
});

// Watch for state ID changes
watch(() => props.stateId, () => {
  items.value = [];
  searchResults.value = [];
  loadedCount.value = 0;
  fetchInitial();
  // Re-subscribe to new state
  if (isSubscribed.value) {
    unsubscribe();
  }
  subscribe();
});

// Watch for view mode changes
watch(viewMode, (mode) => {
  if (mode === 'log') {
    subscribe();
  } else {
    // Keep subscription active even in search mode for background updates
  }
});

onMounted(async () => {
  await fetchInitial();
  subscribe();
  await nextTick();
  setupObserver();
});

onUnmounted(() => {
  if (observer) observer.disconnect();
  unsubscribe();
});
</script>

<template>
  <div class="h-full flex flex-col">
    <!-- Toolbar -->
    <div class="flex items-center gap-4 p-3 border-b border-gray-200 bg-gray-50">
      <!-- View mode tabs -->
      <div class="flex bg-gray-200 rounded-md p-0.5">
        <button
          @click="viewMode = 'log'"
          class="px-3 py-1 text-sm rounded transition-colors"
          :class="viewMode === 'log' ? 'bg-white shadow text-gray-900' : 'text-gray-600 hover:text-gray-900'"
        >
          Log
          <span v-if="isSubscribed" class="ml-1 w-2 h-2 bg-green-500 rounded-full inline-block animate-pulse"></span>
        </button>
        <button
          @click="viewMode = 'search'"
          class="px-3 py-1 text-sm rounded transition-colors"
          :class="viewMode === 'search' ? 'bg-white shadow text-gray-900' : 'text-gray-600 hover:text-gray-900'"
        >
          Search
        </button>
      </div>

      <!-- Search input (only in search mode) -->
      <div v-if="viewMode === 'search'" class="flex-1 flex gap-2">
        <input
          v-model="searchQuery"
          @keyup.enter="doSearch"
          type="text"
          placeholder="Search logs..."
          class="flex-1 px-3 py-1.5 text-sm border border-gray-300 rounded-md"
        />
        <button
          @click="doSearch"
          :disabled="searchLoading"
          class="px-3 py-1.5 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50"
        >
          {{ searchLoading ? 'Searching...' : 'Search' }}
        </button>
      </div>

      <!-- Stats -->
      <div class="text-sm text-gray-500 ml-auto">
        <span v-if="viewMode === 'log'">
          {{ loadedCount }} of {{ totalCount }} loaded
        </span>
        <span v-else>
          {{ totalCount }} total items
        </span>
      </div>

      <!-- Refresh button -->
      <button
        @click="fetchInitial"
        :disabled="loading"
        class="text-sm text-blue-600 hover:text-blue-700"
      >
        Refresh
      </button>
    </div>

    <!-- Error -->
    <div v-if="error" class="p-3 bg-red-50 text-red-700 text-sm">
      {{ error }}
    </div>

    <!-- Log entries with infinite scroll -->
    <div ref="scrollContainer" class="flex-1 overflow-auto font-mono text-sm">
      <!-- Log view -->
      <template v-if="viewMode === 'log'">
        <div
          v-for="(item, idx) in items"
          :key="idx"
          class="border-b border-gray-100"
        >
          <div
            class="flex items-start gap-3 px-3 py-1.5 hover:bg-gray-50 cursor-pointer"
            @click="toggleExpand(idx)"
          >
            <span class="text-gray-400 select-none w-4 shrink-0">{{ isExpanded(idx) ? '\u25bc' : '\u25b6' }}</span>
            <span class="text-gray-400 text-xs w-16 shrink-0">{{ formatTime(item.timestamp) }}</span>
            <span
              v-if="item.type"
              class="text-xs px-1.5 py-0.5 rounded font-medium w-14 text-center shrink-0"
              :class="getTypeColor(item.type)"
            >
              {{ item.type }}
            </span>
            <span class="text-gray-700 flex-1 truncate">{{ item.message || JSON.stringify(item) }}</span>
          </div>
          <div v-if="isExpanded(idx)" class="px-3 pb-3 pt-1 ml-7">
            <JsonViewer :data="item" />
          </div>
        </div>

        <!-- Sentinel for infinite scroll -->
        <div ref="sentinel" class="h-10 flex items-center justify-center">
          <span v-if="loadingMore" class="text-gray-500 text-sm">Loading more...</span>
          <span v-else-if="!hasMore && items.length > 0" class="text-gray-400 text-sm">End of log</span>
        </div>

        <div v-if="items.length === 0 && !loading" class="p-8 text-center text-gray-500">
          No log entries yet
        </div>
      </template>

      <!-- Search results -->
      <template v-if="viewMode === 'search'">
        <div v-if="searchResults.length > 0">
          <div
            v-for="result in searchResults"
            :key="result.index"
            class="border-b border-gray-100"
          >
            <div
              class="flex items-start gap-3 px-3 py-1.5 hover:bg-gray-50 cursor-pointer"
              @click="toggleExpand(result.index + 100000)"
            >
              <span class="text-gray-400 select-none w-4 shrink-0">{{ isExpanded(result.index + 100000) ? '\u25bc' : '\u25b6' }}</span>
              <span class="text-gray-400 text-xs w-10 shrink-0 text-right">#{{ result.index }}</span>
              <span class="text-gray-400 text-xs w-16 shrink-0">{{ formatTime(result.item.timestamp) }}</span>
              <span
                v-if="result.item.type"
                class="text-xs px-1.5 py-0.5 rounded font-medium w-14 text-center shrink-0"
                :class="getTypeColor(result.item.type)"
              >
                {{ result.item.type }}
              </span>
              <span class="text-gray-700 flex-1 truncate">{{ result.item.message || JSON.stringify(result.item) }}</span>
            </div>
            <div v-if="isExpanded(result.index + 100000)" class="px-3 pb-3 pt-1 ml-7">
              <JsonViewer :data="result.item" />
            </div>
          </div>
        </div>
        <div v-else-if="searchQuery && !searchLoading" class="p-8 text-center text-gray-500">
          No results for "{{ searchQuery }}"
        </div>
        <div v-else-if="!searchQuery" class="p-8 text-center text-gray-500">
          Enter a search term to find log entries
        </div>
      </template>

      <!-- Loading -->
      <div v-if="loading" class="p-8 text-center text-gray-500">
        Loading...
      </div>
    </div>
  </div>
</template>
