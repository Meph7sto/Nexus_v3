import { flushPromises, mount } from '@vue/test-utils'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import ClusterView from '../ClusterView.vue'

const { view, saveWorker, join, copyToClipboard, showError } = vi.hoisted(() => ({
  view: vi.fn(), saveWorker: vi.fn(), join: vi.fn(), copyToClipboard: vi.fn(), showError: vi.fn(),
}))
vi.mock('@/api/admin/cluster', () => ({ clusterAPI: { view, saveWorker, join, dataView: async () => null } }))
vi.mock('@/composables/useClipboard', () => ({ useClipboard: () => ({ copyToClipboard }) }))
vi.mock('@/stores/app', () => ({ useAppStore: () => ({ showSuccess: vi.fn(), showError }) }))
vi.mock('vue-i18n', async (original) => ({
  ...await original<typeof import('vue-i18n')>(),
  useI18n: () => ({ t: (key: string) => key, locale: { value: 'en' } }),
}))

const worker = { node_id: 'w1', name: 'West', url: 'https://worker.test', enabled: true,
  weight: 1, max_inflight: 4, tags: [], note: '', use_h2c: false }
const managedWorker = { ...worker, credential_source: 'managed', has_token: true }
const desired = { version: 8, policy: { enabled: true }, workers: [managedWorker] }

function mountCluster() {
  return mount(ClusterView, { global: { stubs: {
    AppLayout: { template: '<main><slot /></main>' },
    BaseDialog: { props: ['show'], emits: ['close'], template: `
      <section v-if="show">
        <button type="button" aria-label="Close modal" @click="$emit('close')" />
        <slot />
      </section>`,
    },
    TotpStepUpDialog: { props: ['controller'], template: `
      <div v-if="controller.visible.value">
        <button type="button" data-test="cancel-step-up" @click="controller.onCancel()" />
        <button type="button" data-test="verify-step-up" @click="controller.onVerified()" />
      </div>`,
    },
    ConfirmDialog: true, Icon: true, StatusBadge: true,
  } } })
}

let wrapper: ReturnType<typeof mountCluster>

beforeEach(async () => {
  vi.resetAllMocks()
  view.mockResolvedValue({ desired, entries: [], bindings: [] })
  saveWorker.mockResolvedValue({ ...desired, version: 9 })
  wrapper = mountCluster()
  await flushPromises()
})

afterEach(() => wrapper.unmount())

function click(key: string) {
  return wrapper.get(`button[aria-label="admin.cluster.${key}"]`).trigger('click')
}

function addWorker() {
  const button = wrapper.findAll('button').find((candidate) => candidate.text() === 'admin.cluster.addWorker')!
  expect(button.element.disabled).toBe(false)
  return button.trigger('click')
}

it('creates a managed Worker with no approved bindings using a locally generated and copied token', async () => {
  await addWorker()
  expect(wrapper.find('select[name="secret_ref"]').exists()).toBe(false)
  await wrapper.get('input[name="node_id"]').setValue('west-2')
  await wrapper.get('input[name="name"]').setValue('West 2')
  await wrapper.get('input[name="url"]').setValue('http://10.1.2.3:8081')
  const transport = wrapper.get('select[name="use_h2c"]')
  await transport.setValue(transport.findAll('option')[1].element.value)
  await wrapper.get('input[name="weight"]').setValue('3')
  await wrapper.get('input[name="max_inflight"]').setValue('64')
  await wrapper.get('input[name="tags"]').setValue(' west, gateway, ')
  await wrapper.get('textarea[name="note"]').setValue('private network')
  await wrapper.get('input[name="enabled"]').setValue(true)
  await wrapper.findAll('button').find((button) => button.text() === 'admin.cluster.generateToken')!.trigger('click')
  const secret = wrapper.get('input[name="shared_token"]')
  expect(secret.attributes('type')).toBe('password')
  const token = secret.element.value
  expect(token).toMatch(/^[0-9a-f]{48}$/)
  await click('copyToken')
  expect(copyToClipboard).toHaveBeenCalledWith(token)
  await wrapper.get('form').trigger('submit')
  await flushPromises()
  expect(saveWorker).toHaveBeenCalledWith({ node_id: 'west-2', name: 'West 2', url: 'http://10.1.2.3:8081',
    use_h2c: true, shared_token: token, weight: 3, max_inflight: 64, tags: ['west', 'gateway'],
    note: 'private network', enabled: true }, 8, false)
  expect(wrapper.find('form').exists()).toBe(false)
  await addWorker()
  expect(wrapper.get('input[name="shared_token"]').element.value).toBe('')
})

it('publishes a zero scheduling limit against the version opened for editing and retains the stored token', async () => {
  await click('editWorker')
  expect(wrapper.get('input[name="shared_token"]').element.value).toBe('')
  expect(wrapper.get('input[name="node_id"]').element.readOnly).toBe(true)
  const capacity = wrapper.get('input[name="max_inflight"]')
  await capacity.setValue('0')
  expect(capacity.element.checkValidity()).toBe(true)
  await wrapper.get('form').trigger('submit')
  await flushPromises()
  expect(saveWorker).toHaveBeenCalledWith({ ...worker, max_inflight: 0 }, 8, true)
})

it.each([
  { label: 'preserves the binding when blank', token: '', credentials: { secret_ref: 'west-file' } },
  { label: 'switches to managed credentials with a new token', token: 'a'.repeat(32), credentials: { shared_token: 'a'.repeat(32) } },
])('editing a preconfigured Worker $label', async ({ token, credentials }) => {
  view.mockResolvedValueOnce({
    desired: { ...desired, workers: [{ ...worker, credential_source: 'binding', has_token: true, secret_ref: 'west-file' }] },
    entries: [], bindings: [],
  })
  await wrapper.get('button[aria-label="common.refresh"]').trigger('click')
  await flushPromises()
  await click('editWorker')
  expect(wrapper.get('#worker-token-hint').text()).toBe('admin.cluster.editBoundTokenHint')
  expect(wrapper.get('input[name="url"]').element.readOnly).toBe(true)
  expect(wrapper.get('select[name="use_h2c"]').element.disabled).toBe(true)
  const secret = wrapper.get('input[name="shared_token"]')
  expect(secret.element.value).toBe('')
  expect(secret.element.required).toBe(false)
  await secret.setValue(token)
  await wrapper.get('form').trigger('submit')
  await flushPromises()
  expect(saveWorker).toHaveBeenCalledWith({ ...worker, ...credentials }, 8, true)
  expect(wrapper.find('input[name="shared_token"]').exists()).toBe(false)
})

it('keeps a failed replacement token masked and uses it unchanged on retry', async () => {
  saveWorker.mockRejectedValueOnce(new Error('Connection interrupted'))
  await click('editWorker')
  const token = 'a'.repeat(31) + '!'
  const secret = wrapper.get('input[name="shared_token"]')
  await secret.setValue(token)
  await wrapper.get('form').trigger('submit')
  await flushPromises()
  expect(saveWorker).toHaveBeenNthCalledWith(1, { ...worker, shared_token: token }, 8, true)
  expect(showError).toHaveBeenCalledWith('Connection interrupted')
  expect(secret.element.value).toBe(token)
  expect(secret.element.type).toBe('password')
  expect(wrapper.text()).not.toContain(token)

  await wrapper.get('form').trigger('submit')
  await flushPromises()
  expect(saveWorker).toHaveBeenNthCalledWith(2, { ...worker, shared_token: token }, 8, true)
  expect(wrapper.find('form').exists()).toBe(false)
  await click('editWorker')
  expect(wrapper.get('input[name="shared_token"]').element.value).toBe('')
})

it('keeps the same token after step-up cancellation and clears it after a verified retry', async () => {
  const stepUpRequired = { status: 403, reason: 'STEP_UP_REQUIRED' }
  saveWorker.mockRejectedValueOnce(stepUpRequired).mockRejectedValueOnce(stepUpRequired)
  await click('editWorker')
  const token = 'b'.repeat(32)
  const secret = wrapper.get('input[name="shared_token"]')
  await secret.setValue(token)
  await wrapper.get('form').trigger('submit')
  await flushPromises()
  await wrapper.get('[data-test="cancel-step-up"]').trigger('click')
  await flushPromises()
  expect(saveWorker).toHaveBeenCalledTimes(1)
  expect(showError).not.toHaveBeenCalled()
  expect(secret.element.value).toBe(token)
  expect(secret.element.type).toBe('password')
  expect(wrapper.text()).not.toContain(token)

  await wrapper.get('form').trigger('submit')
  await flushPromises()
  await wrapper.get('[data-test="verify-step-up"]').trigger('click')
  await flushPromises()
  expect(saveWorker.mock.calls).toEqual(Array.from({ length: 3 }, () => [{ ...worker, shared_token: token }, 8, true]))
  expect(wrapper.find('form').exists()).toBe(false)
  await click('editWorker')
  expect(wrapper.get('input[name="shared_token"]').element.value).toBe('')
})

it('rejects malformed tokens and clears the draft when closed', async () => {
  await click('editWorker')
  for (const malformedToken of ['short', `${'a'.repeat(32)},`, `${'a'.repeat(32)} `, `${'a'.repeat(32)}é`]) {
    await wrapper.get('input[name="shared_token"]').setValue(malformedToken)
    await wrapper.get('form').trigger('submit')
    expect(wrapper.get('[role="alert"]').text()).toBe('admin.cluster.invalidToken')
  }
  expect(saveWorker).not.toHaveBeenCalled()
  await wrapper.get('button[aria-label="Close modal"]').trigger('click')
  await click('editWorker')
  expect(wrapper.get('input[name="shared_token"]').element.value).toBe('')
})

it('continues to register preconfigured bindings without asking for the token', async () => {
  view.mockResolvedValueOnce({ desired, entries: [], bindings: [{ ref: 'east', node_id: 'w2', url: 'http://10.1.2.4:8081' }] })
  await wrapper.get('button[aria-label="common.refresh"]').trigger('click')
  await flushPromises()
  await addWorker()
  await wrapper.get('select[name="secret_ref"]').setValue('east')
  await wrapper.get('input[name="name"]').setValue('East')
  expect(wrapper.find('input[name="shared_token"]').exists()).toBe(false)
  expect(wrapper.get('input[name="node_id"]').element.value).toBe('w2')
  expect(wrapper.get('input[name="url"]').element.readOnly).toBe(true)
  await wrapper.get('form').trigger('submit')
  expect(saveWorker).toHaveBeenCalledWith({ node_id: 'w2', name: 'East', url: 'http://10.1.2.4:8081',
    use_h2c: true, secret_ref: 'east', weight: 1, max_inflight: 32, tags: [], note: '', enabled: false }, 8, false)
})

it('adds token-file instructions to the deployment template for a managed Worker', async () => {
  join.mockResolvedValue({ template: 'WORKER_NODE_ID=w1' })
  await click('join')
  await flushPromises()
  expect(join).toHaveBeenCalledWith('w1')
  expect(wrapper.text()).toContain('admin.cluster.joinTokenFile')
  expect(wrapper.get('pre').text()).toBe('WORKER_NODE_ID=w1')
})
