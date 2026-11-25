<template>
  <AppLayout>
    <div class="mx-auto w-full max-w-6xl space-y-6">
      <h2 class="text-lg font-semibold text-gray-900 lg:hidden">{{ label('title') }}</h2>
      <dl v-if="view" class="grid grid-cols-2 gap-x-5 gap-y-4 border-y border-gray-200 py-4 sm:grid-cols-4">
        <div v-for="metric in metrics" :key="metric.key" class="min-w-0">
          <dt class="text-xs font-medium text-gray-500">{{ label(metric.key) }}</dt>
          <dd class="mt-1 break-words text-xl font-semibold tabular-nums text-gray-900">{{ metric.value }}</dd>
        </div>
      </dl>

      <nav class="flex flex-wrap gap-2 border-b border-gray-200 pb-2" :aria-label="label('title')">
        <button v-for="item in tabs" :key="item.key" type="button"
          class="inline-flex items-center gap-2 rounded-md px-3 py-2 text-sm font-medium"
          :class="tab === item.key ? 'bg-primary-50 text-primary-700' : 'text-gray-600 hover:bg-gray-100'"
          :aria-pressed="tab === item.key" @click="tab = item.key">
          <Icon :name="item.icon" size="sm" /> {{ label(item.key) }}
        </button>
        <button type="button" class="btn btn-secondary ml-auto" :disabled="loading" :title="t('common.refresh')"
          :aria-label="t('common.refresh')" @click="refresh">
          <Icon name="refresh" size="sm" :class="{ 'animate-spin': loading }" />
        </button>
      </nav>

      <p v-if="!view && tab !== 'data'" class="cluster-empty" role="status">
        {{ loading ? t('common.loading') : label('unavailable') }}
      </p>
      <section v-if="view && tab === 'workers'" class="space-y-4">
        <div class="flex flex-wrap items-center justify-between gap-3">
          <h3 class="text-base font-semibold text-gray-900">{{ label('workerPool') }}</h3>
          <div class="flex flex-wrap gap-2">
            <button type="button" class="btn btn-secondary" :disabled="busy" @click="openPolicy">
              <Icon name="cog" size="sm" /> {{ label('policy') }}
            </button>
            <button type="button" class="btn btn-primary" :disabled="busy" @click="openWorker()">
              <Icon name="plus" size="sm" /> {{ label('addWorker') }}
            </button>
          </div>
        </div>
        <p v-if="!workers.length" class="cluster-empty">{{ label('noWorkers') }}</p>
        <div v-else class="overflow-x-auto">
          <table class="cluster-table min-w-[710px]">
            <thead><tr>
              <th scope="col">{{ label('worker') }}</th><th scope="col">{{ label('address') }}</th>
              <th scope="col">{{ label('status') }}</th><th scope="col">{{ label('capacity') }}</th>
              <th scope="col">{{ label('actions') }}</th>
            </tr></thead>
            <tbody>
              <tr v-for="node in workers" :key="node.node_id">
                <td>
                  <div class="font-medium text-gray-900">{{ node.name || node.node_id }}</div>
                  <div class="text-xs text-gray-500">{{ node.node_id }}</div>
                  <div v-if="node.tags?.length" class="mt-1 text-xs text-gray-500">{{ node.tags.join(', ') }}</div>
                </td>
                <td :data-label="label('address')"><span class="break-all font-mono text-xs">{{ node.url }}</span></td>
                <td>
                  <button v-if="node.drain_version" type="button" class="whitespace-nowrap text-sm text-amber-700 hover:underline"
                    @click="openDrain(node.node_id)">
                    {{ label('drainTitle') }}
                  </button>
                  <StatusBadge v-else :status="node.enabled ? 'active' : 'disabled'"
                    :label="label(node.enabled ? 'enabled' : 'disabled')" />
                </td>
                <td class="tabular-nums" :data-label="label('capacity')">{{ node.max_inflight }}</td>
                <td>
                  <div class="flex flex-wrap gap-1">
                    <button v-for="item in workerActions" :key="item.key" type="button" class="cluster-icon"
                      :aria-label="label(item.key)" :title="label(item.key)" :disabled="busy" @click="item.run(node)">
                      <Icon :name="item.icon" size="sm" />
                    </button>
                  </div>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <section v-if="view && tab === 'entries'" class="space-y-4">
        <h3 class="text-base font-semibold text-gray-900">{{ label('onlineEntries') }}</h3>
        <p v-if="!entries.length" class="cluster-empty">{{ label('noEntries') }}</p>
        <div v-else class="overflow-x-auto">
          <table class="cluster-table min-w-[680px]">
            <thead><tr>
              <th scope="col">Entry</th><th scope="col">{{ label('role') }}</th>
              <th scope="col">{{ label('applied') }}</th><th scope="col">{{ label('workload') }}</th>
              <th scope="col">{{ label('heartbeat') }}</th>
            </tr></thead>
            <tbody>
              <tr v-for="entry in entries" :key="entry.instance_id">
                <td>
                  <div class="font-medium text-gray-900">{{ entry.name || entry.node_id }}</div>
                  <div class="max-w-44 break-all font-mono text-xs text-gray-500">{{ entry.instance_id }}</div>
                  <div class="text-xs text-gray-500">{{ entry.version }}</div>
                </td>
                <td :data-label="label('role')">{{ entry.role }}</td>
                <td>
                  <StatusBadge :status="entry.apply_error ? 'error' : entryConfirmed(entry) ? 'success' : 'warning'"
                    :label="label(entry.apply_error ? 'failed' : entryConfirmed(entry) ? 'confirmed' : 'awaiting')" />
                  <p class="font-mono text-xs">{{ entry.applied_version }} / {{ view.desired.version }}</p>
                  <p v-if="entry.apply_error" class="max-w-52 break-words text-xs text-rose-700">{{ entry.apply_error }}</p>
                </td>
                <td>
                  <div>{{ label('activeConnections') }}: {{ entry.active_connections }}</div>
                  <div>{{ label('heap') }}: {{ entry.mem_alloc_mb.toFixed(1) }} MiB</div>
                  <div>{{ label('sharedTasks') }}: {{ label(entry.shared_tasks_running ? 'active' : 'inactive') }}</div>
                </td>
                <td :data-label="label('heartbeat')">{{ formatDate(entry.updated_at) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <section v-if="tab === 'data'" class="space-y-5">
        <div v-if="dataSummary" class="grid gap-4 border-b border-gray-200 pb-4 sm:grid-cols-3">
          <div>
            <h3 class="text-sm font-semibold text-gray-900">{{ label('pgPool') }}</h3>
            <p class="mt-1 text-sm text-gray-600">
              {{ label(dataSummary.postgres.status) }} · {{ label('active') }} {{ dataSummary.pg_active }}
              · {{ label('pgIdle') }} {{ dataSummary.pg_idle }}
            </p>
            <p class="text-sm text-gray-600">
              {{ label('replicas') }}:
              {{ dataSummary.postgres.replica_state === 'measured'
                ? `${dataSummary.postgres.streaming} / ${dataSummary.postgres.replicas}`
                : label(dataSummary.postgres.replica_state) }}
            </p>
          </div>
          <div>
            <h3 class="text-sm font-semibold text-gray-900">Redis</h3>
            <p class="mt-1 text-sm text-gray-600">
              {{ label(dataSummary.redis.status) }}{{ dataSummary.redis.mode ? ' · ' + label(dataSummary.redis.mode) : '' }}
            </p>
            <p v-if="dataSummary.redis.primary" class="break-all font-mono text-xs text-gray-600">{{ dataSummary.redis.primary }}</p>
          </div>
          <div>
            <h3 class="text-sm font-semibold text-gray-900">{{ label('backup') }}</h3>
            <p class="mt-1 text-sm text-gray-600">
              {{ label(dataSummary.backup.status) }}
              <template v-if="dataSummary.backup.started_at"> · {{ formatDate(dataSummary.backup.started_at) }}</template>
            </p>
          </div>
        </div>
        <h3 class="text-base font-semibold text-gray-900">{{ label('dataTitle') }}</h3>
        <p v-if="!dataSummary?.profiles.length" class="cluster-empty">
          {{ loading ? t('common.loading') : dataSummary ? label('noData') : label('refreshFailed') }}
        </p>
        <form v-else class="grid max-w-3xl gap-4 sm:grid-cols-2" @submit.prevent="runPreflight">
          <label class="cluster-field">{{ label('chooseData') }}
            <select v-model="dataProfile" class="input" :disabled="busy" required>
              <option value="" disabled>{{ label('pickData') }}</option>
              <option v-for="profile in dataSummary.profiles" :key="profile.id" :value="profile.id">
                {{ profile.label }} · {{ label(profile.mode) }}
              </option>
            </select>
          </label>
          <label class="cluster-field">{{ label('changeType') }}
            <select v-model="intent" class="input" :disabled="busy">
              <option value="same_cluster">{{ label('same_cluster') }}</option>
              <option value="migration">{{ label('migration') }}</option>
            </select>
          </label>
          <div class="flex flex-wrap gap-2 sm:col-span-2">
            <button type="submit" class="btn btn-primary" :disabled="busy || !dataProfile">
              <Icon name="bolt" size="sm" /> {{ label('preflight') }}
            </button>
            <button type="button" class="btn btn-secondary" :disabled="busy || !preflight?.ready" @click="exportData">
              <Icon name="download" size="sm" /> {{ label('export') }}
            </button>
          </div>
        </form>
        <div v-if="preflight" class="max-w-3xl border-t border-gray-200 pt-4" aria-live="polite">
          <StatusBadge :status="preflight.ready ? 'success' : 'error'"
            :label="label(preflight.ready ? 'dataReady' : 'dataBlocked')" />
          <p v-if="preflight.redis_primary" class="mt-3 break-all text-sm text-gray-600">
            {{ label('redisPrimary') }}: <span class="font-mono">{{ preflight.redis_primary }}</span>
          </p>
          <ul class="mt-3 divide-y divide-gray-100">
            <li v-for="check in preflight.checks" :key="check.key" class="flex items-center gap-2 py-2 text-sm text-gray-700">
              <Icon :name="check.ok ? 'checkCircle' : 'xCircle'" size="sm"
                :class="check.ok ? 'text-emerald-700' : 'text-rose-700'" />
              <span>{{ label(check.key) }}</span>
              <span v-if="check.key === 'postgres_migrations'" class="text-xs text-gray-500">({{ preflight.migration_count }})</span>
              <span class="sr-only">{{ label(check.ok ? 'confirmed' : 'failed') }}</span>
            </li>
          </ul>
        </div>
      </section>

      <BaseDialog :show="!!workerDraft" :title="label(editingWorker ? 'editWorker' : 'addWorker')"
        :close-on-escape="!busy" :show-close-button="!busy" @close="closeWorker">
        <form v-if="workerDraft" @submit.prevent="saveWorker">
          <fieldset class="grid min-w-0 gap-4 sm:grid-cols-2" :disabled="busy">
            <label v-if="!editingWorker && unusedBindings.length" class="cluster-field sm:col-span-2">{{ label('credentialSource') }}
              <select v-model="workerDraft.secret_ref" class="input" name="secret_ref" @change="applyWorkerBinding">
                <option value="">{{ label('managedWorker') }}</option>
                <optgroup :label="label('binding')">
                  <option v-for="binding in unusedBindings" :key="binding.ref" :value="binding.ref">
                    {{ binding.node_id }} · {{ binding.url }}
                  </option>
                </optgroup>
              </select>
            </label>
            <label class="cluster-field">{{ label('nodeID') }}
              <input v-model.trim="workerDraft.node_id" class="input font-mono" name="node_id" maxlength="64"
                pattern="[A-Za-z0-9._\-]{1,64}" :readonly="editingWorker || !!workerDraft.secret_ref" required />
            </label>
            <label class="cluster-field">{{ label('name') }}
              <input v-model.trim="workerDraft.name" class="input" name="name" maxlength="120" required />
            </label>
            <label class="cluster-field">{{ label('transport') }}
              <select v-model="workerDraft.use_h2c" class="input" name="use_h2c" :disabled="!!workerDraft.secret_ref"
                @change="workerError = ''">
                <option :value="false">{{ label('mtls') }}</option>
                <option :value="true">{{ label('h2c') }}</option>
              </select>
            </label>
            <label class="cluster-field">{{ label('address') }}
              <input v-model.trim="workerDraft.url" class="input font-mono" name="url" type="url" maxlength="350"
                :placeholder="workerDraft.use_h2c ? 'http://10.0.0.8:9090' : 'https://worker.internal:9090'"
                :readonly="!!workerDraft.secret_ref" :aria-describedby="!workerDraft.secret_ref ? 'worker-endpoint-hint' : undefined"
                required @input="workerError = ''" />
            </label>
            <p v-if="!workerDraft.secret_ref" id="worker-endpoint-hint" class="-mt-2 text-xs text-gray-500 sm:col-span-2">
              {{ label(workerDraft.use_h2c ? 'h2cHint' : 'mtlsHint') }} {{ label('workerDNSHint') }}
            </p>
            <div v-if="editingWorker || !workerDraft.secret_ref" class="min-w-0 space-y-1.5 sm:col-span-2">
              <label class="cluster-field" for="worker-shared-token">{{ label('sharedToken') }}</label>
              <div class="flex flex-wrap gap-2">
                <input id="worker-shared-token" v-model="sharedToken" class="input min-w-[8rem] flex-1 font-mono"
                  name="shared_token" type="password" autocomplete="new-password" spellcheck="false"
                  aria-describedby="worker-token-hint worker-token-file" :required="!editingWorker" @input="workerError = ''" />
                <button type="button" class="btn btn-secondary" @click="generateToken">
                  <Icon name="key" size="sm" /> {{ label('generateToken') }}
                </button>
                <button type="button" class="btn btn-secondary" :disabled="!sharedToken"
                  :aria-label="label('copyToken')" :title="label('copyToken')" @click="copyToClipboard(sharedToken)">
                  <Icon name="copy" size="sm" />
                </button>
              </div>
              <p id="worker-token-hint" class="text-xs text-gray-500">{{ label(workerTokenHint) }}</p>
              <p id="worker-token-file" class="break-words text-xs text-gray-500">{{ label('tokenFile', { path: workerTokenFile }) }}</p>
            </div>
            <label class="cluster-field">{{ label('weight') }}<input v-model.number="workerDraft.weight" class="input"
              name="weight" type="number" min="1" max="10000" required /></label>
            <label class="cluster-field">{{ label('capacity') }}<input v-model.number="workerDraft.max_inflight" class="input"
              name="max_inflight" type="number" min="0" max="65535" required /></label>
            <label class="cluster-field sm:col-span-2">{{ label('tags') }}<input v-model="tagsInput" class="input" name="tags" /></label>
            <label class="cluster-field sm:col-span-2">{{ label('note') }}<textarea v-model.trim="workerDraft.note"
              class="input" name="note" rows="2" maxlength="1000" /></label>
            <p v-if="workerError" class="text-sm text-rose-700 sm:col-span-2" role="alert">{{ label(workerError) }}</p>
            <div class="flex flex-wrap items-center justify-between gap-3 sm:col-span-2">
              <label class="flex items-center gap-2 text-sm"><input v-model="workerDraft.enabled" type="checkbox" name="enabled" />
                {{ label('enabled') }}</label>
              <button type="submit" class="btn btn-primary">{{ label('publish') }}</button>
            </div>
          </fieldset>
        </form>
      </BaseDialog>

      <BaseDialog :show="!!policyDraft" :title="label('policy')" @close="policyDraft = null">
        <form v-if="policyDraft" class="grid gap-4 sm:grid-cols-2" @submit.prevent="savePolicy">
          <label class="flex items-center gap-2 text-sm"><input v-model="policyDraft.enabled" type="checkbox" />
            {{ label('enabled') }}</label>
          <label class="flex items-center gap-2 text-sm"><input v-model="policyDraft.fallback_local" type="checkbox" />
            {{ label('fallback_local') }}</label>
          <label class="cluster-field sm:col-span-2">{{ label('strategy') }}
            <select v-model="policyDraft.selection_policy" class="input">
              <option v-for="strategy in strategies" :key="strategy" :value="strategy">{{ label(strategy) }}</option>
            </select>
          </label>
          <label v-for="field in policyNumbers" :key="field" class="cluster-field">{{ label(field) }}
            <input v-model.number="policyDraft[field]" class="input" type="number" min="1" required />
          </label>
          <button type="submit" class="btn btn-primary self-end" :disabled="busy">{{ label('publish') }}</button>
        </form>
      </BaseDialog>

      <ConfirmDialog :show="!!confirmAction" :title="label(confirmAction?.kind || 'remove')"
        :message="label(confirmAction?.kind === 'drain' ? 'drainRequest' : 'removeRequest', { name: confirmAction?.node.name || '' })"
        :danger="confirmAction?.kind === 'remove'" @cancel="confirmAction = null" @confirm="runConfirmed" />

      <BaseDialog :show="!!probe" :title="label('probeTitle')" @close="probe = null">
        <dl v-if="probe" class="cluster-health">
          <div><dt>{{ label('worker') }}</dt><dd>{{ probe.node_id }}</dd></div>
          <div><dt>{{ label('status') }}</dt><dd>{{ probe.status === 'ok' ? label('healthy') : probe.status }}</dd></div>
          <div><dt>{{ label('version') }}</dt><dd>{{ probe.version }}</dd></div>
          <div><dt>{{ label('inFlight') }}</dt><dd>{{ probe.in_flight }} / {{ probe.max_inflight }}</dd></div>
          <div><dt>{{ label('heap') }}</dt><dd>{{ probe.mem_alloc_mb.toFixed(1) }} MiB</dd></div>
          <div><dt>{{ label('roundTrip') }}</dt><dd>{{ probe.rtt_ms }} ms</dd></div>
        </dl>
      </BaseDialog>

      <BaseDialog :show="!!drainID" :title="label('drainTitle')" @close="closeDrain">
        <StatusBadge :status="drainComplete ? 'success' : 'warning'"
          :label="label(drainComplete ? 'drainComplete' : 'drainPending')" />
        <dl v-if="drainStatus" class="cluster-health mt-4">
          <div><dt>{{ label('desired') }}</dt><dd>{{ drainStatus.version }}</dd></div>
          <div><dt>{{ label('inFlight') }}</dt><dd>{{ drainStatus.health?.in_flight ?? '-' }}</dd></div>
        </dl>
        <p v-if="drainStatus?.pending_entries?.length" class="mt-4 break-all text-sm text-amber-700">
          {{ label('pendingEntries') }}: {{ drainStatus.pending_entries.join(', ') }}
        </p>
        <p v-else-if="!drainComplete" class="mt-4 text-sm text-gray-500">{{ label('waitingWorker') }}</p>
        <p v-if="drainStatus?.uncertain_entries?.length" class="mt-2 break-all text-sm text-amber-700">
          {{ label('uncertainEntries') }}: {{ drainStatus.uncertain_entries.join(', ') }}
        </p>
        <template #footer>
          <button type="button" class="btn btn-secondary" @click="loadDrain">
            <Icon name="refresh" size="sm" /> {{ t('common.refresh') }}
          </button>
        </template>
      </BaseDialog>

      <BaseDialog :show="template !== null" :title="label('join')" @close="template = null">
        <p class="mb-2 text-sm">{{ label('joinRoutes') }}</p>
        <p v-if="template?.managed" class="mb-2 break-words text-sm">{{ label('joinTokenFile', { path: workerTokenFile }) }}</p>
        <pre class="max-w-full whitespace-pre-wrap break-words rounded-md bg-gray-100 p-3 text-xs">{{ template?.text }}</pre>
      </BaseDialog>
      <TotpStepUpDialog :controller="stepUp" />
    </div>
  </AppLayout>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import AppLayout from '@/components/layout/AppLayout.vue'
import BaseDialog from '@/components/common/BaseDialog.vue'
import ConfirmDialog from '@/components/common/ConfirmDialog.vue'
import StatusBadge from '@/components/common/StatusBadge.vue'
import TotpStepUpDialog from '@/components/auth/TotpStepUpDialog.vue'
import Icon from '@/components/icons/Icon.vue'
import { useClipboard } from '@/composables/useClipboard'
import { useStepUp, isStepUpCancelled } from '@/composables/useStepUp'
import { useAppStore } from '@/stores/app'
import {
  clusterAPI, type ClusterSnapshot, type ClusterView, type DataPreflight, type DataRequest,
  type DataSummary, type EntryHeartbeat, type WorkerConfig, type WorkerDrainStatus, type WorkerNode,
  type WorkerPoolPolicy, type WorkerProbe, type WorkerSaveInput,
} from '@/api/admin/cluster'

const { t, locale } = useI18n()
const label = (key: string, params: Record<string, string> = {}) => t(`admin.cluster.${key}`, params)
const app = useAppStore()
const stepUp = useStepUp()
const { copyToClipboard } = useClipboard()
const tabs = [{ key: 'workers', icon: 'server' }, { key: 'entries', icon: 'grid' }, { key: 'data', icon: 'database' }] as const
const tab = ref<(typeof tabs)[number]['key']>('workers')
const view = ref<ClusterView | null>(null)
const dataSummary = ref<DataSummary | null>(null)
const loading = ref(false)
const busy = ref(false)
const workers = computed(() => view.value?.desired.workers || [])
const entries = computed(() => view.value?.entries || [])
const unusedBindings = computed(() => (view.value?.bindings || []).filter((b) => !workers.value.some((n) => n.node_id === b.node_id)))
const entryConfirmed = (entry: EntryHeartbeat) => !entry.apply_error && entry.applied_version === view.value?.desired.version
const metrics = computed(() => [
  { key: 'desired', value: view.value?.desired.version ?? '-' },
  { key: 'onlineEntries', value: entries.value.length },
  { key: 'appliedCount', value: entries.value.length ? `${entries.value.filter(entryConfirmed).length} / ${entries.value.length}` : '-' },
  { key: 'workerPool', value: label(view.value?.desired.policy.enabled ? 'enabled' : 'localMode') },
])

const workerDraft = ref<WorkerConfig | null>(null)
const editingWorker = ref(false)
const tagsInput = ref('')
const sharedToken = ref('')
const workerError = ref('')
const workerTokenFile = 'WORKER_SECRETS_DIR/shared-token'
const workerTokenHint = computed(() => {
  if (!editingWorker.value) return 'tokenHint'
  return workerDraft.value?.secret_ref ? 'editBoundTokenHint' : 'editTokenHint'
})
const workerVersion = ref(0)
const policyDraft = ref<WorkerPoolPolicy | null>(null)
const policyVersion = ref(0)
const strategies = ['least_inflight_affinity', 'least_inflight', 'round_robin']
const policyNumbers = ['health_interval_seconds', 'health_timeout_seconds', 'circuit_failure_threshold', 'circuit_open_seconds', 'max_attempts'] as const
const confirmAction = ref<{ kind: 'drain' | 'remove'; node: WorkerNode; version: number } | null>(null)
const probe = ref<WorkerProbe | null>(null)
const template = ref<{ text: string; managed: boolean } | null>(null)
const drainID = ref('')
const drainStatus = ref<WorkerDrainStatus | null>(null)
const drainComplete = computed(() => drainStatus.value?.complete && drainStatus.value.health?.status === 'draining' &&
  drainStatus.value.health.in_flight === 0 && !drainStatus.value.pending_entries?.length &&
  !drainStatus.value.uncertain_entries?.length)
const dataProfile = ref('')
const intent = ref<DataRequest['intent']>('same_cluster')
const preflight = ref<DataPreflight | null>(null)
let drainTimer: ReturnType<typeof setInterval> | undefined
let refreshTimer: ReturnType<typeof setInterval> | undefined
let drainRequestSequence = 0

const workerActions = [
  { key: 'probe', icon: 'bolt', run: probeWorker },
  { key: 'editWorker', icon: 'edit', run: openWorker },
  { key: 'drain', icon: 'clock', run: requestDrain },
  { key: 'join', icon: 'terminal', run: showJoin },
  { key: 'remove', icon: 'trash', run: (node: WorkerNode) => confirm('remove', node) },
] as const

function formatDate(date: string | undefined) {
  return date && !Number.isNaN(Date.parse(date)) ? new Date(date).toLocaleString(locale.value) : '-'
}

async function refresh() {
  if (loading.value) return
  loading.value = true
  try {
    const [cluster, data] = await Promise.allSettled([clusterAPI.view(), clusterAPI.dataView()])
    if (cluster.status === 'fulfilled') {
      if (!view.value || cluster.value.desired.version >= view.value.desired.version) view.value = cluster.value
    } else {
      view.value = null
    }
    dataSummary.value = data.status === 'fulfilled' ? data.value : null
  } finally {
    loading.value = false
  }
}

async function runAction(action: () => Promise<void>) {
  if (busy.value) return
  busy.value = true
  try {
    await action()
  } catch (error) {
    if (!isStepUpCancelled(error)) {
      app.showError((error as { message?: string })?.message || label('actionFailed'))
      if ((error as { status?: number })?.status === 409) void refresh()
    }
  } finally {
    busy.value = false
  }
}

function publish(snapshot: ClusterSnapshot) {
  if (view.value && snapshot.version >= view.value.desired.version) view.value = { ...view.value, desired: snapshot }
  app.showSuccess(label('published'))
  void refresh()
}

function isManagedWorker(node: WorkerNode) {
  return node.credential_source ? node.credential_source === 'managed' : !node.secret_ref
}

function closeWorker() {
  sharedToken.value = ''
  workerError.value = ''
  workerDraft.value = null
}

function openWorker(node?: WorkerNode) {
  if (!view.value || busy.value) return
  closeWorker()
  editingWorker.value = !!node
  workerVersion.value = view.value.desired.version
  workerDraft.value = node ? {
    node_id: node.node_id, name: node.name, url: node.url, use_h2c: node.use_h2c,
    secret_ref: isManagedWorker(node) ? '' : node.secret_ref,
    weight: node.weight, max_inflight: node.max_inflight, tags: node.tags, note: node.note, enabled: node.enabled,
  } : {
    node_id: '', name: '', url: '', use_h2c: false, secret_ref: '',
    weight: 1, max_inflight: 32, tags: [], note: '', enabled: false,
  }
  tagsInput.value = node?.tags?.join(', ') || ''
}

function applyWorkerBinding() {
  if (!workerDraft.value) return
  sharedToken.value = ''
  workerError.value = ''
  const binding = unusedBindings.value.find((item) => item.ref === workerDraft.value?.secret_ref)
  if (!binding) return
  workerDraft.value.node_id = binding.node_id
  workerDraft.value.url = binding.url
  workerDraft.value.use_h2c = binding.url.startsWith('http://')
}

function generateToken() {
  try {
    const bytes = globalThis.crypto.getRandomValues(new Uint8Array(24))
    sharedToken.value = Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('')
    workerError.value = ''
  } catch {
    workerError.value = 'tokenGenerationFailed'
  }
}

function saveWorker() {
  if (!workerDraft.value || busy.value) return
  workerError.value = ''
  const existing = editingWorker.value
  const version = workerVersion.value
  const { secret_ref, ...fields } = workerDraft.value
  const node: WorkerSaveInput = { ...fields, tags: tagsInput.value.split(',').map((tag) => tag.trim()).filter(Boolean) }
  const token = sharedToken.value
  if (secret_ref && (!existing || !token)) {
    node.secret_ref = secret_ref
  } else {
    if (!node.url.startsWith(node.use_h2c ? 'http://' : 'https://')) {
      workerError.value = node.use_h2c ? 'h2cEndpointError' : 'mtlsEndpointError'
      return
    }
    if ((!existing || token) && (token.length < 32 || /[^\x21-\x2b\x2d-\x7e]/.test(token))) {
      workerError.value = 'invalidToken'
      return
    }
    if (token) node.shared_token = token
  }
  void runAction(async () => {
    const result = await stepUp.run(() => clusterAPI.saveWorker(node, version, existing))
    closeWorker()
    publish(result)
  })
}

function openPolicy() {
  if (!view.value) return
  policyVersion.value = view.value.desired.version
  policyDraft.value = { ...view.value.desired.policy }
}

function savePolicy() {
  if (!policyDraft.value) return
  const draft = { ...policyDraft.value }
  void runAction(async () => {
    const result = await stepUp.run(() => clusterAPI.publishPolicy(draft, policyVersion.value))
    policyDraft.value = null
    publish(result)
  })
}

function confirm(kind: 'drain' | 'remove', node: WorkerNode) {
  if (view.value) confirmAction.value = { kind, node, version: view.value.desired.version }
}

function requestDrain(node: WorkerNode) {
  if (node.drain_version) openDrain(node.node_id)
  else confirm('drain', node)
}

function runConfirmed() {
  const action = confirmAction.value
  if (!action) return
  confirmAction.value = null
  const id = action.node.node_id
  void runAction(async () => {
    const result = await stepUp.run(() => action.kind === 'drain'
      ? clusterAPI.drain(id, action.version)
      : clusterAPI.removeWorker(id, action.version))
    publish(result)
    if (action.kind === 'drain') openDrain(id)
  })
}

function probeWorker(node: WorkerNode) {
  void runAction(async () => { probe.value = await stepUp.run(() => clusterAPI.probe(node.node_id)) })
}

function showJoin(node: WorkerNode) {
  void runAction(async () => {
    const result = await stepUp.run(() => clusterAPI.join(node.node_id))
    template.value = { text: result.template, managed: isManagedWorker(node) }
  })
}

function closeDrain() {
  drainRequestSequence++
  drainID.value = ''
  drainStatus.value = null
  if (drainTimer !== undefined) clearInterval(drainTimer)
  drainTimer = undefined
}

function openDrain(id: string) {
  closeDrain()
  drainID.value = id
  void loadDrain()
  drainTimer = setInterval(loadDrain, 4_000)
}

async function loadDrain() {
  const id = drainID.value
  if (!id) return
  const sequence = ++drainRequestSequence
  try {
    const status = await clusterAPI.drainStatus(id)
    if (drainID.value === id && sequence === drainRequestSequence) drainStatus.value = status
  } catch {
    if (drainID.value === id && sequence === drainRequestSequence) drainStatus.value = null
  }
}

function dataRequest(): DataRequest {
  return { profile_id: dataProfile.value, intent: intent.value }
}

function runPreflight() {
  if (!dataProfile.value) return
  preflight.value = null
  void runAction(async () => { preflight.value = await stepUp.run(() => clusterAPI.preflight(dataRequest())) })
}

function exportData() {
  if (!preflight.value?.ready) return
  const request = dataRequest()
  void runAction(async () => {
    const result = await stepUp.run(() => clusterAPI.exportData(request))
    const url = URL.createObjectURL(new Blob([JSON.stringify(result, null, 2)], { type: 'application/json' }))
    const link = document.createElement('a')
    link.href = url
    link.download = `nexus-data-${request.profile_id}.json`
    link.click()
    setTimeout(() => URL.revokeObjectURL(url), 1_000)
  })
}

watch([dataProfile, intent], () => { preflight.value = null })
onMounted(() => {
  void refresh()
  refreshTimer = setInterval(refresh, 10_000)
})
onUnmounted(() => {
  closeWorker()
  closeDrain()
  if (refreshTimer !== undefined) clearInterval(refreshTimer)
})
</script>

<style scoped>
.cluster-table {
  @apply w-full table-auto text-left text-sm text-gray-700;
}
.cluster-table th {
  @apply border-b border-gray-200 bg-gray-50 px-3 py-2 text-xs font-semibold text-gray-600;
}
.cluster-table td {
  @apply border-b border-gray-100 px-3 py-3 align-top;
}
.cluster-icon {
  @apply inline-flex h-8 w-8 items-center justify-center rounded-md text-gray-600 hover:bg-gray-100 focus-visible:outline-primary-500 disabled:opacity-50;
}
.cluster-field {
  @apply flex min-w-0 flex-col gap-1.5 text-sm font-medium text-gray-700;
}
.cluster-empty {
  @apply border-y border-gray-200 py-8 text-center text-sm text-gray-500;
}
.cluster-health {
  @apply grid grid-cols-2 gap-3 text-sm;
}
.cluster-health dt {
  @apply text-xs text-gray-500;
}
.cluster-health dd {
  @apply break-words font-medium tabular-nums text-gray-900;
}
@media (max-width: 639px) {
  .cluster-table {
    @apply block min-w-0;
  }
  .cluster-table thead {
    @apply sr-only;
  }
  .cluster-table tbody {
    @apply block;
  }
  .cluster-table tr {
    @apply grid grid-cols-2 gap-3 border-b border-gray-200 py-3;
  }
  .cluster-table td {
    @apply min-w-0 break-words border-0 p-0;
  }
  .cluster-table td:last-child {
    @apply col-span-2;
  }
  .cluster-table td[data-label]::before {
    @apply mb-0.5 block text-xs font-medium text-gray-500;
    content: attr(data-label);
  }
}
</style>
