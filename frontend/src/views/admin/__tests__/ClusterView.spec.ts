import { flushPromises, mount } from '@vue/test-utils'
import { expect, it, vi } from 'vitest'
import ClusterView from '../ClusterView.vue'

const { view, saveWorker } = vi.hoisted(() => ({ view: vi.fn(), saveWorker: vi.fn() }))
vi.mock('@/api/admin/cluster', () => ({ clusterAPI: { view, saveWorker, dataView: async () => null } }))
vi.mock('@/stores/app', () => ({ useAppStore: () => ({ showSuccess: vi.fn(), showError: vi.fn() }) }))
vi.mock('@/composables/useStepUp', () => ({
  useStepUp: () => ({ run: (fn: () => unknown) => fn() }), isStepUpCancelled: () => false,
}))
vi.mock('vue-i18n', async (original) => ({
  ...await original<typeof import('vue-i18n')>(),
  useI18n: () => ({ t: (key: string) => key, locale: { value: 'en' } }),
}))

it('publishes a zero scheduling limit against the version opened for editing', async () => {
  const node = { node_id: 'w1', name: 'West', secret_ref: 'w1', url: 'https://worker.test', enabled: true,
    weight: 1, max_inflight: 4, tags: [], note: '', use_h2c: false }
  const desired = { version: 8, policy: { enabled: true }, workers: [node] }
  view.mockResolvedValue({ desired, entries: [], bindings: [] })
  saveWorker.mockResolvedValue({ ...desired, version: 9 })
  const wrapper = mount(ClusterView, { global: { stubs: {
    AppLayout: { template: '<main><slot /></main>' },
    BaseDialog: { props: ['show'], template: '<section v-if="show"><slot /></section>' },
    ConfirmDialog: true, TotpStepUpDialog: true, Icon: true, StatusBadge: true,
  } } })
  try {
    await flushPromises()
    await wrapper.get('button[aria-label="admin.cluster.editWorker"]').trigger('click')
    const capacity = wrapper.get('input[type="number"][max="65535"]')
    await capacity.setValue('0')
    expect((capacity.element as HTMLInputElement).checkValidity()).toBe(true)
    await wrapper.get('form').trigger('submit')
    await flushPromises()
    expect(saveWorker).toHaveBeenCalledWith({ ...node, max_inflight: 0 }, 8, true)
  } finally {
    wrapper.unmount()
  }
})
