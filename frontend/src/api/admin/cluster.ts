import { apiClient } from '../client'

export interface WorkerConfig {
  node_id: string
  name: string
  url: string
  use_h2c: boolean
  weight: number
  max_inflight: number
  tags: string[]
  note: string
  enabled: boolean
  secret_ref?: string
}

export interface WorkerSaveInput extends WorkerConfig {
  shared_token?: string
}

export interface WorkerNode extends WorkerConfig {
  has_token?: boolean
  credential_source?: 'managed' | 'binding'
  drain_version?: number
}

export interface WorkerPoolPolicy {
  enabled: boolean
  selection_policy: string
  health_interval_seconds: number
  health_timeout_seconds: number
  circuit_failure_threshold: number
  circuit_open_seconds: number
  max_attempts: number
  fallback_local: boolean
}

export interface EntryHeartbeat {
  instance_id: string
  node_id: string
  name: string
  role: string
  version: string
  applied_version: number
  apply_error?: string
  mem_alloc_mb: number
  active_connections: number
  shared_tasks_running: boolean
  updated_at: string
}

export interface WorkerBinding {
  ref: string
  node_id: string
  url: string
}

export interface ClusterSnapshot {
  version: number
  workers: WorkerNode[]
  policy: WorkerPoolPolicy
}

export interface ClusterView {
  desired: ClusterSnapshot
  entries: EntryHeartbeat[]
  bindings: WorkerBinding[]
}

export interface WorkerProbe {
  status: string
  node_id: string
  version: string
  in_flight: number
  max_inflight: number
  mem_alloc_mb: number
  rtt_ms: number
}

export interface WorkerDrainStatus {
  version: number
  pending_entries: string[]
  uncertain_entries?: string[]
  complete: boolean
  health?: WorkerProbe
}

export interface DataSummary {
  profiles: { id: string; label: string; mode: string }[]
  pg_active: number
  pg_idle: number
  postgres: { status: string; replica_state: string; streaming: number; replicas: number }
  redis: { status: string; mode?: string; primary?: string }
  backup: { status: string; started_at?: string }
}

export interface DataRequest {
  profile_id: string
  intent: 'same_cluster' | 'migration'
}

export interface DataPreflight {
  profile_id: string
  intent: DataRequest['intent']
  mode: string
  ready: boolean
  migration_count: number
  redis_primary?: string
  checks: { key: string; ok: boolean }[]
}

export interface DataExport {
  preflight: DataPreflight
  config: Record<string, unknown>
  required_env: string[]
}

const base = '/admin/cluster'
const worker = (id: string) => `${base}/workers/${encodeURIComponent(id)}`

export const clusterAPI = {
  view: () => apiClient.get<ClusterView>(base).then((r) => r.data),
  saveWorker: (node: WorkerSaveInput, expected_version: number, existing: boolean) => {
    const body = { node, expected_version }
    return (existing
      ? apiClient.put<ClusterSnapshot>(worker(node.node_id), body)
      : apiClient.post<ClusterSnapshot>(`${base}/workers`, body)
    ).then((r) => r.data)
  },
  removeWorker: (id: string, expected_version: number) =>
    apiClient.delete<ClusterSnapshot>(worker(id), { params: { expected_version } }).then((r) => r.data),
  publishPolicy: (policy: WorkerPoolPolicy, expected_version: number) =>
    apiClient.put<ClusterSnapshot>(`${base}/policy`, { policy, expected_version }).then((r) => r.data),
  probe: (id: string) => apiClient.post<WorkerProbe>(`${worker(id)}/probe`).then((r) => r.data),
  join: (id: string) => apiClient.post<{ template: string }>(`${worker(id)}/join`).then((r) => r.data),
  drain: (id: string, expected_version: number) =>
    apiClient.post<ClusterSnapshot>(`${worker(id)}/drain`, { expected_version }).then((r) => r.data),
  drainStatus: (id: string) => apiClient.get<WorkerDrainStatus>(`${worker(id)}/drain`).then((r) => r.data),
  dataView: () => apiClient.get<DataSummary>(`${base}/data`).then((r) => r.data),
  preflight: (request: DataRequest) => apiClient.post<DataPreflight>(`${base}/data/preflight`, request).then((r) => r.data),
  exportData: (request: DataRequest) => apiClient.post<DataExport>(`${base}/data/export`, request).then((r) => r.data),
}
