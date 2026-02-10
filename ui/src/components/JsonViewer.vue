<script setup lang="ts">
import { ref, computed, watch } from 'vue';

const props = defineProps<{
  data: unknown;
  rawText?: string;
}>();

const viewMode = ref<'formatted' | 'raw'>('formatted');
const expandedPaths = ref<Set<string>>(new Set());
const defaultExpanded = ref(true);
// Track expanded long strings separately (default collapsed)
const expandedStrings = ref<Set<string>>(new Set());

// Detect if data is structured (object/array) vs plain text/binary
const isStructured = computed(() => {
  return props.data !== null && typeof props.data === 'object';
});

const isBinary = computed(() => {
  if (typeof props.data === 'string') return false;
  if (Array.isArray(props.data) && props.data.length > 0) {
    // Check if it looks like a byte array
    return props.data.every(v => typeof v === 'number' && v >= 0 && v <= 255);
  }
  return false;
});

const rawDisplay = computed(() => {
  if (props.rawText !== undefined) return props.rawText;
  if (typeof props.data === 'string') return props.data;
  try {
    return JSON.stringify(props.data, null, 2);
  } catch {
    return String(props.data);
  }
});

// Reset expanded state when data changes
watch(() => props.data, () => {
  expandedPaths.value = new Set();
  expandedStrings.value = new Set();
  defaultExpanded.value = true;
});

function isStringExpanded(path: string): boolean {
  return expandedStrings.value.has(path);
}

function toggleStringExpand(path: string) {
  if (expandedStrings.value.has(path)) {
    expandedStrings.value.delete(path);
  } else {
    expandedStrings.value.add(path);
  }
  // Trigger reactivity
  expandedStrings.value = new Set(expandedStrings.value);
}

function isExpanded(path: string): boolean {
  if (expandedPaths.value.has(path)) {
    return !defaultExpanded.value;
  }
  return defaultExpanded.value;
}

function toggleExpand(path: string) {
  if (expandedPaths.value.has(path)) {
    expandedPaths.value.delete(path);
  } else {
    expandedPaths.value.add(path);
  }
}

function expandAll() {
  expandedPaths.value = new Set();
  defaultExpanded.value = true;
}

function collapseAll() {
  expandedPaths.value = new Set();
  defaultExpanded.value = false;
}

function getType(value: unknown): string {
  if (value === null) return 'null';
  if (value === undefined) return 'undefined';
  if (Array.isArray(value)) return 'array';
  return typeof value;
}

function formatPrimitive(value: unknown): string {
  if (typeof value === 'string') return `"${value}"`;
  if (value === null) return 'null';
  if (value === undefined) return 'undefined';
  return String(value);
}

async function copyToClipboard() {
  try {
    await navigator.clipboard.writeText(rawDisplay.value);
  } catch (e) {
    console.error('Failed to copy:', e);
  }
}
</script>

<template>
  <div class="text-sm">
    <!-- Toolbar -->
    <div class="flex items-center gap-2 mb-2 text-xs">
      <button
        @click="viewMode = 'formatted'"
        :class="[
          'px-2 py-1 rounded transition-colors',
          viewMode === 'formatted'
            ? 'bg-blue-600 text-white'
            : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
        ]"
        :disabled="!isStructured"
        :title="!isStructured ? 'Content is not structured data' : 'Formatted view with highlighting'"
      >
        Formatted
      </button>
      <button
        @click="viewMode = 'raw'"
        :class="[
          'px-2 py-1 rounded transition-colors',
          viewMode === 'raw'
            ? 'bg-blue-600 text-white'
            : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
        ]"
      >
        Raw
      </button>
      <template v-if="viewMode === 'formatted' && isStructured">
        <span class="text-gray-400">|</span>
        <button
          @click="expandAll"
          class="px-2 py-1 rounded bg-gray-200 text-gray-700 hover:bg-gray-300 transition-colors"
        >
          Expand All
        </button>
        <button
          @click="collapseAll"
          class="px-2 py-1 rounded bg-gray-200 text-gray-700 hover:bg-gray-300 transition-colors"
        >
          Collapse All
        </button>
      </template>
      <button
        @click="copyToClipboard"
        class="ml-auto px-2 py-1 rounded bg-gray-200 text-gray-700 hover:bg-gray-300 transition-colors"
        title="Copy to clipboard"
      >
        Copy
      </button>
    </div>

    <!-- Content -->
    <div class="bg-gray-100 rounded p-3 overflow-auto text-sm font-mono">
      <!-- Raw view -->
      <pre v-if="viewMode === 'raw' || !isStructured" class="text-gray-700 whitespace-pre-wrap break-all">{{ rawDisplay }}</pre>

      <!-- Formatted view with highlighting -->
      <div v-else class="leading-relaxed">
        <JsonNode :value="data" :path="'$'" :is-expanded="isExpanded" :toggle-expand="toggleExpand" :is-string-expanded="isStringExpanded" :toggle-string-expand="toggleStringExpand" />
      </div>
    </div>

    <!-- Binary warning -->
    <div v-if="isBinary" class="mt-2 text-xs text-amber-600">
      Content appears to be binary data (byte array)
    </div>
  </div>
</template>

<!-- Recursive component for JSON tree -->
<script lang="ts">
import { defineComponent, h, type PropType, type VNode } from 'vue';

interface JsonNodeProps {
  value: unknown;
  path: string;
  keyName?: string;
  isLast?: boolean;
  isExpanded: (path: string) => boolean;
  toggleExpand: (path: string) => void;
  isStringExpanded: (path: string) => boolean;
  toggleStringExpand: (path: string) => void;
}

function getType(val: unknown): string {
  if (val === null) return 'null';
  if (val === undefined) return 'undefined';
  if (Array.isArray(val)) return 'array';
  return typeof val;
}

function renderJsonNode(props: JsonNodeProps): VNode {
  const type = getType(props.value);
  const comma = props.isLast !== false ? '' : ',';

  // Key prefix (if this node has a key)
  const keyPrefix = props.keyName
    ? h('span', { class: 'text-purple-600' }, [`"${props.keyName}"`, h('span', { class: 'text-gray-400' }, ': ')])
    : null;

  // Primitives
  if (type === 'string') {
    const strVal = props.value as string;
    const truncateThreshold = 500;
    const isLong = strVal.length > truncateThreshold;
    const expanded = props.isStringExpanded(props.path);

    if (!isLong) {
      // Short string - render normally
      return h('div', { class: 'whitespace-nowrap' }, [
        keyPrefix,
        h('span', { class: 'text-green-700' }, `"${strVal}"`),
        h('span', { class: 'text-gray-400' }, comma),
      ]);
    }

    // Long string - render with expand/collapse toggle
    if (!expanded) {
      // Collapsed view - show truncated with expand button
      const truncated = strVal.slice(0, truncateThreshold);
      return h('div', { class: 'whitespace-nowrap' }, [
        h('span', {
          class: 'cursor-pointer select-none text-gray-400 hover:text-gray-600 mr-1',
          onClick: (e: Event) => { e.stopPropagation(); props.toggleStringExpand(props.path); },
          title: 'Click to expand',
        }, '\u25b6'),
        keyPrefix,
        h('span', { class: 'text-green-700' }, `"${truncated}`),
        h('span', {
          class: 'text-blue-500 cursor-pointer hover:underline mx-1',
          onClick: (e: Event) => { e.stopPropagation(); props.toggleStringExpand(props.path); },
        }, `... [${strVal.length - truncateThreshold} more chars]`),
        h('span', { class: 'text-green-700' }, '"'),
        h('span', { class: 'text-gray-400' }, comma),
      ]);
    }

    // Expanded view - show full string with collapse button
    return h('div', { class: 'whitespace-normal' }, [
      h('div', { class: 'flex items-start' }, [
        h('span', {
          class: 'cursor-pointer select-none text-gray-400 hover:text-gray-600 mr-1 flex-shrink-0',
          onClick: (e: Event) => { e.stopPropagation(); props.toggleStringExpand(props.path); },
          title: 'Click to collapse',
        }, '\u25bc'),
        h('div', { class: 'flex-1 min-w-0' }, [
          keyPrefix,
          h('span', { class: 'text-green-700 break-all' }, `"${strVal}"`),
          h('span', { class: 'text-gray-400' }, comma),
          h('span', {
            class: 'text-blue-500 cursor-pointer hover:underline ml-2 text-xs',
            onClick: (e: Event) => { e.stopPropagation(); props.toggleStringExpand(props.path); },
          }, '[collapse]'),
        ]),
      ]),
    ]);
  }

  if (type === 'number') {
    return h('div', { class: 'whitespace-nowrap' }, [
      keyPrefix,
      h('span', { class: 'text-blue-600' }, String(props.value)),
      h('span', { class: 'text-gray-400' }, comma),
    ]);
  }

  if (type === 'boolean') {
    return h('div', { class: 'whitespace-nowrap' }, [
      keyPrefix,
      h('span', { class: 'text-amber-600' }, String(props.value)),
      h('span', { class: 'text-gray-400' }, comma),
    ]);
  }

  if (type === 'null' || type === 'undefined') {
    return h('div', { class: 'whitespace-nowrap' }, [
      keyPrefix,
      h('span', { class: 'text-gray-500 italic' }, type),
      h('span', { class: 'text-gray-400' }, comma),
    ]);
  }

  // Arrays
  if (type === 'array') {
    const arr = props.value as unknown[];
    const expanded = props.isExpanded(props.path);

    if (arr.length === 0) {
      return h('div', { class: 'whitespace-nowrap' }, [
        keyPrefix,
        h('span', { class: 'text-gray-400' }, '[]'),
        h('span', { class: 'text-gray-400' }, comma),
      ]);
    }

    if (!expanded) {
      return h('div', { class: 'whitespace-nowrap' }, [
        h('span', {
          class: 'cursor-pointer select-none text-gray-400 hover:text-gray-600 mr-1',
          onClick: () => props.toggleExpand(props.path),
        }, '\u25b6'),
        keyPrefix,
        h('span', { class: 'text-gray-400' }, '['),
        h('span', { class: 'text-gray-500 italic' }, ` ${arr.length} items `),
        h('span', { class: 'text-gray-400' }, ']'),
        h('span', { class: 'text-gray-400' }, comma),
      ]);
    }

    return h('div', { class: 'whitespace-nowrap' }, [
      h('div', { class: 'whitespace-nowrap' }, [
        h('span', {
          class: 'cursor-pointer select-none text-gray-400 hover:text-gray-600 mr-1',
          onClick: () => props.toggleExpand(props.path),
        }, '\u25bc'),
        keyPrefix,
        h('span', { class: 'text-gray-400' }, '['),
      ]),
      h('div', { class: 'pl-4 border-l border-gray-300 ml-2' },
        arr.map((item, idx) =>
          renderJsonNode({
            value: item,
            path: `${props.path}[${idx}]`,
            isLast: idx === arr.length - 1,
            isExpanded: props.isExpanded,
            toggleExpand: props.toggleExpand,
            isStringExpanded: props.isStringExpanded,
            toggleStringExpand: props.toggleStringExpand,
          })
        )
      ),
      h('div', { class: 'whitespace-nowrap' }, [
        h('span', { class: 'text-gray-400' }, ']'),
        h('span', { class: 'text-gray-400' }, comma),
      ]),
    ]);
  }

  // Objects
  if (type === 'object') {
    const obj = props.value as Record<string, unknown>;
    const keys = Object.keys(obj);
    const expanded = props.isExpanded(props.path);

    if (keys.length === 0) {
      return h('div', { class: 'whitespace-nowrap' }, [
        keyPrefix,
        h('span', { class: 'text-gray-400' }, '{}'),
        h('span', { class: 'text-gray-400' }, comma),
      ]);
    }

    if (!expanded) {
      return h('div', { class: 'whitespace-nowrap' }, [
        h('span', {
          class: 'cursor-pointer select-none text-gray-400 hover:text-gray-600 mr-1',
          onClick: () => props.toggleExpand(props.path),
        }, '\u25b6'),
        keyPrefix,
        h('span', { class: 'text-gray-400' }, '{'),
        h('span', { class: 'text-gray-500 italic' }, ` ${keys.length} keys `),
        h('span', { class: 'text-gray-400' }, '}'),
        h('span', { class: 'text-gray-400' }, comma),
      ]);
    }

    return h('div', { class: 'whitespace-nowrap' }, [
      h('div', { class: 'whitespace-nowrap' }, [
        h('span', {
          class: 'cursor-pointer select-none text-gray-400 hover:text-gray-600 mr-1',
          onClick: () => props.toggleExpand(props.path),
        }, '\u25bc'),
        keyPrefix,
        h('span', { class: 'text-gray-400' }, '{'),
      ]),
      h('div', { class: 'pl-4 border-l border-gray-300 ml-2' },
        keys.map((key, idx) =>
          renderJsonNode({
            value: obj[key],
            path: `${props.path}.${key}`,
            keyName: key,
            isLast: idx === keys.length - 1,
            isExpanded: props.isExpanded,
            toggleExpand: props.toggleExpand,
            isStringExpanded: props.isStringExpanded,
            toggleStringExpand: props.toggleStringExpand,
          })
        )
      ),
      h('div', { class: 'whitespace-nowrap' }, [
        h('span', { class: 'text-gray-400' }, '}'),
        h('span', { class: 'text-gray-400' }, comma),
      ]),
    ]);
  }

  // Fallback
  return h('div', { class: 'whitespace-nowrap text-gray-400' }, String(props.value));
}

const JsonNode = defineComponent({
  name: 'JsonNode',
  props: {
    value: { type: null as unknown as PropType<unknown>, required: true },
    path: { type: String, required: true },
    keyName: { type: String, default: '' },
    isLast: { type: Boolean, default: true },
    isExpanded: { type: Function as PropType<(path: string) => boolean>, required: true },
    toggleExpand: { type: Function as PropType<(path: string) => void>, required: true },
    isStringExpanded: { type: Function as PropType<(path: string) => boolean>, required: true },
    toggleStringExpand: { type: Function as PropType<(path: string) => void>, required: true },
  },
  setup(props): () => VNode {
    return (): VNode => renderJsonNode({
      value: props.value,
      path: props.path,
      keyName: props.keyName,
      isLast: props.isLast,
      isExpanded: props.isExpanded,
      toggleExpand: props.toggleExpand,
      isStringExpanded: props.isStringExpanded,
      toggleStringExpand: props.toggleStringExpand,
    });
  },
});

export { JsonNode };
</script>
