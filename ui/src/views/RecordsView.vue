<script setup lang="ts">
import { onMounted, watch, ref, nextTick, onUnmounted } from 'vue';
import { useRecordsStore } from '@/stores/records';
import { useBranchesStore } from '@/stores/branches';

const recordsStore = useRecordsStore();
const branchesStore = useBranchesStore();

const scrollContainer = ref<HTMLElement | null>(null);
const sentinel = ref<HTMLElement | null>(null);
let observer: IntersectionObserver | null = null;

// Resizable detail panel
const detailPanelHeight = ref(256);
const isResizing = ref(false);
const minPanelHeight = 100;
const maxPanelHeight = 600;

function startResize(e: MouseEvent) {
  isResizing.value = true;
  const startY = e.clientY;
  const startHeight = detailPanelHeight.value;

  function onMouseMove(e: MouseEvent) {
    const delta = startY - e.clientY;
    detailPanelHeight.value = Math.min(maxPanelHeight, Math.max(minPanelHeight, startHeight + delta));
  }

  function onMouseUp() {
    isResizing.value = false;
    document.removeEventListener('mousemove', onMouseMove);
    document.removeEventListener('mouseup', onMouseUp);
  }

  document.addEventListener('mousemove', onMouseMove);
  document.addEventListener('mouseup', onMouseUp);
}

// Decode a byte array to string, attempting JSON parse for pretty formatting
function decodeBytes(bytes: number[]): unknown {
  try {
    const str = new TextDecoder().decode(new Uint8Array(bytes));
    try {
      return JSON.parse(str);
    } catch {
      return str;
    }
  } catch {
    return bytes;
  }
}

// Recursively decode byte arrays in operation objects
function decodeOperations(obj: unknown): unknown {
  if (obj === null || obj === undefined) return obj;
  if (Array.isArray(obj)) {
    // Check if it's a byte array (all numbers 0-255)
    if (obj.length > 0 && obj.every(v => typeof v === 'number' && v >= 0 && v <= 255)) {
      return decodeBytes(obj);
    }
    return obj.map(decodeOperations);
  }
  if (typeof obj === 'object') {
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(obj)) {
      // Decode operation payloads
      if ((key === 'Set' || key === 'Append' || key === 'Merge') && Array.isArray(value)) {
        result[key] = decodeBytes(value as number[]);
      } else {
        result[key] = decodeOperations(value);
      }
    }
    return result;
  }
  return obj;
}

function formatPayload(payload: unknown): string {
  if (typeof payload === 'string') return payload;
  if (payload instanceof ArrayBuffer || ArrayBuffer.isView(payload)) {
    return `[Binary: ${(payload as ArrayBuffer).byteLength || (payload as Uint8Array).length} bytes]`;
  }
  try {
    const decoded = decodeOperations(payload);
    return JSON.stringify(decoded, null, 2);
  } catch {
    return String(payload);
  }
}

function formatTimestamp(ts: number): string {
  // Chronicle stores timestamps as microseconds, JS Date expects milliseconds
  return new Date(ts / 1000).toLocaleString();
}

function handleTypeChange(event: Event) {
  const value = (event.target as HTMLSelectElement).value;
  recordsStore.setTypeFilter(value || null);
}

function selectRecord(record: typeof recordsStore.records[0]) {
  recordsStore.selectedRecord = record;
}

function setupObserver() {
  if (observer) observer.disconnect();

  observer = new IntersectionObserver(
    (entries) => {
      if (entries[0]?.isIntersecting && recordsStore.hasMore && !recordsStore.loadingMore) {
        recordsStore.loadMore();
      }
    },
    { root: scrollContainer.value, threshold: 0.1 }
  );

  if (sentinel.value) {
    observer.observe(sentinel.value);
  }
}

onMounted(async () => {
  await recordsStore.fetchRecords();
  recordsStore.fetchRecordTypes();
  await nextTick();
  setupObserver();
});

onUnmounted(() => {
  if (observer) observer.disconnect();
});

// Refetch when branch changes
watch(() => branchesStore.currentBranch, () => {
  recordsStore.fetchRecords();
});
</script>

<template>
  <div class="h-full flex flex-col">
    <!-- Toolbar -->
    <div class="border-b border-gray-200 bg-gray-50 px-4 py-3">
      <div class="flex items-center gap-4">
        <div class="flex items-center gap-2">
          <label class="text-sm text-gray-600">Type:</label>
          <select
            :value="recordsStore.typeFilter || ''"
            @change="handleTypeChange"
            class="text-sm border border-gray-300 rounded px-2 py-1"
          >
            <option value="">All types</option>
            <option v-for="t in recordsStore.recordTypes" :key="t" :value="t">{{ t }}</option>
          </select>
        </div>
        <button
          @click="recordsStore.fetchRecords()"
          :disabled="recordsStore.loading"
          class="text-sm px-3 py-1 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
        >
          {{ recordsStore.loading ? 'Loading...' : 'Refresh' }}
        </button>
        <div class="text-sm text-gray-500 ml-auto">
          {{ recordsStore.loadedCount }} of {{ recordsStore.totalCount }} loaded
        </div>
        <div v-if="recordsStore.error" class="text-sm text-red-600">
          {{ recordsStore.error }}
        </div>
      </div>
    </div>

    <!-- Records List -->
    <div ref="scrollContainer" class="flex-1 overflow-auto">
      <table class="w-full text-sm">
        <thead class="bg-gray-100 sticky top-0">
          <tr>
            <th class="text-left px-4 py-2 font-medium text-gray-700">Seq</th>
            <th class="text-left px-4 py-2 font-medium text-gray-700">Type</th>
            <th class="text-left px-4 py-2 font-medium text-gray-700">Timestamp</th>
            <th class="text-left px-4 py-2 font-medium text-gray-700">Payload</th>
            <th class="text-left px-4 py-2 font-medium text-gray-700">Links</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="record in recordsStore.records"
            :key="record.id"
            class="border-b border-gray-100 hover:bg-gray-50 cursor-pointer"
            :class="recordsStore.selectedRecord?.id === record.id ? 'bg-blue-50' : ''"
            @click="selectRecord(record)"
          >
            <td class="px-4 py-2 font-mono text-gray-600">{{ record.sequence }}</td>
            <td class="px-4 py-2">
              <span class="px-2 py-0.5 bg-gray-200 rounded text-xs font-medium">{{ record.recordType }}</span>
            </td>
            <td class="px-4 py-2 text-gray-600">{{ formatTimestamp(record.timestamp) }}</td>
            <td class="px-4 py-2 font-mono text-xs text-gray-700 max-w-md truncate">
              {{ formatPayload(record.payload).slice(0, 100) }}{{ formatPayload(record.payload).length > 100 ? '...' : '' }}
            </td>
            <td class="px-4 py-2 text-gray-500 text-xs">
              <span v-if="record.causedBy?.length" class="mr-2">caused:{{ record.causedBy.length }}</span>
              <span v-if="record.linkedTo?.length">links:{{ record.linkedTo.length }}</span>
            </td>
          </tr>
          <tr v-if="recordsStore.records.length === 0 && !recordsStore.loading">
            <td colspan="5" class="px-4 py-8 text-center text-gray-500">
              No records found
            </td>
          </tr>
        </tbody>
      </table>

      <!-- Sentinel for infinite scroll -->
      <div ref="sentinel" class="h-10 flex items-center justify-center">
        <span v-if="recordsStore.loadingMore" class="text-gray-500 text-sm">Loading more...</span>
        <span v-else-if="!recordsStore.hasMore && recordsStore.records.length > 0" class="text-gray-400 text-sm">End of records</span>
      </div>
    </div>

    <!-- Detail Panel -->
    <div v-if="recordsStore.selectedRecord" class="border-t border-gray-200 bg-white flex flex-col" :style="{ height: detailPanelHeight + 'px' }">
      <!-- Resize Handle -->
      <div
        @mousedown="startResize"
        class="h-1.5 cursor-ns-resize bg-gray-200 hover:bg-blue-400 transition-colors flex-shrink-0"
        :class="isResizing ? 'bg-blue-500' : ''"
      />
      <div class="p-4 overflow-auto flex-1">
        <div class="flex items-center justify-between mb-3">
          <h3 class="font-semibold text-gray-900">Record Details</h3>
          <button
            @click="recordsStore.clearSelection()"
            class="text-gray-400 hover:text-gray-600"
          >
            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
        <div class="grid grid-cols-2 gap-4 text-sm">
          <div>
            <span class="text-gray-500">ID:</span>
            <span class="ml-2 font-mono">{{ recordsStore.selectedRecord.id }}</span>
          </div>
          <div>
            <span class="text-gray-500">Sequence:</span>
            <span class="ml-2">{{ recordsStore.selectedRecord.sequence }}</span>
          </div>
          <div>
            <span class="text-gray-500">Type:</span>
            <span class="ml-2">{{ recordsStore.selectedRecord.recordType }}</span>
          </div>
          <div>
            <span class="text-gray-500">Timestamp:</span>
            <span class="ml-2">{{ formatTimestamp(recordsStore.selectedRecord.timestamp) }}</span>
          </div>
          <div v-if="recordsStore.selectedRecord.causedBy?.length">
            <span class="text-gray-500">Caused By:</span>
            <span class="ml-2 font-mono">{{ recordsStore.selectedRecord.causedBy.join(', ') }}</span>
          </div>
          <div v-if="recordsStore.selectedRecord.linkedTo?.length">
            <span class="text-gray-500">Linked To:</span>
            <span class="ml-2 font-mono">{{ recordsStore.selectedRecord.linkedTo.join(', ') }}</span>
          </div>
        </div>
        <div class="mt-4 flex-1 flex flex-col min-h-0">
          <span class="text-gray-500 text-sm">Payload:</span>
          <pre class="mt-1 p-3 bg-gray-100 rounded text-xs font-mono overflow-auto flex-1">{{ formatPayload(recordsStore.selectedRecord.payload) }}</pre>
        </div>
      </div>
    </div>
  </div>
</template>
