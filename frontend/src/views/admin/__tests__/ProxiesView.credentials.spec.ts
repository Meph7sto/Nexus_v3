import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, shallowMount } from '@vue/test-utils'
import ProxiesView from '../ProxiesView.vue'

const { list, update, getAllWithCount, copyToClipboard } = vi.hoisted(() => ({ list: vi.fn(), update: vi.fn(), getAllWithCount: vi.fn(), copyToClipboard: vi.fn() }))
vi.mock('@/api/admin', () => ({ adminAPI: { proxies: { list, update, getAllWithCount } } }))
vi.mock('@/stores/app', () => ({ useAppStore: () => ({ showError: vi.fn(), showSuccess: vi.fn() }) }))
vi.mock('@/stores/auth', () => ({ useAuthStore: () => ({ canAdmin: () => true }) }))
vi.mock('@/composables/useClipboard', () => ({ useClipboard: () => ({ copyToClipboard }) }))
vi.mock('vue-i18n', async () => ({
  ...await vi.importActual<typeof import('vue-i18n')>('vue-i18n'),
  useI18n: () => ({ t: (key: string) => key }),
}))
const mountView = () => shallowMount(ProxiesView, {
  global: { stubs: {
    AppLayout: { template: '<div><slot /></div>' },
    TablePageLayout: { template: '<div><slot name="table" /></div>' },
    DataTable: {
      props: ['data'],
      template: '<div v-for="row in data" :key="row.id"><slot name="cell-actions" :row="row" /><slot name="cell-auth" :row="row" /><slot name="cell-address" :row="row" /></div>',
    },
    BaseDialog: { props: ['show'], template: '<div v-if="show"><slot /><slot name="footer" /></div>' },
    AdminPermissionGate: { template: '<slot />' },
  } },
})
let wrapper: ReturnType<typeof mountView>
beforeEach(() => {
  vi.clearAllMocks()
  list.mockResolvedValue({ items: [{ id: 9, name: 'proxy', protocol: 'http', host: 'proxy.example', port: 8080, username: 'old-user', password: 'fixture-secret', has_credentials: true, status: 'active' }], total: 1, pages: 1 })
  getAllWithCount.mockResolvedValue([])
  update.mockResolvedValue({})
})
afterEach(() => wrapper?.unmount())
async function edit() {
  wrapper = mountView(); await flushPromises()
  await wrapper.findAll('button').find(button => button.text() === 'common.edit')!.trigger('click')
}
async function submit() {
  await wrapper.get('#edit-proxy-form').trigger('submit'); await flushPromises()
  expect(update).toHaveBeenCalledTimes(1)
  return update.mock.calls[0][1]
}

describe('proxy credential updates', () => {
  it('shows passwords only on request and copies the authenticated URL', async () => {
    wrapper = mountView()
    await flushPromises()
    expect(wrapper.text()).toContain('old-user')
    expect(wrapper.text()).not.toContain('fixture-secret')
    await wrapper.get('button[aria-label="admin.proxies.showPassword"]').trigger('click')
    expect(wrapper.text()).toContain('fixture-secret')
    await wrapper.get('button[aria-label="admin.proxies.hidePassword"]').trigger('click')
    expect(wrapper.text()).not.toContain('fixture-secret')
    await wrapper.get('button[title="admin.proxies.copyProxyUrl"]').trigger('click')
    expect(copyToClipboard).toHaveBeenCalledWith('http://old-user:fixture-secret@proxy.example:8080', 'admin.proxies.urlCopied')
  })
  it('sends an explicit empty username when cleared', async () => {
    await edit()
    const username = wrapper.get('#edit-proxy-form input[autocomplete="username"]')
    await username.setValue('')
    const payload = await submit()
    expect(payload.username).toBe('')
    expect(payload).not.toHaveProperty('password')
  })

  it('sends an explicit empty password when the user clears the field', async () => {
    await edit()
    await wrapper.get('#edit-proxy-form input[type="password"]').setValue('')
    expect((await submit()).password).toBe('')
  })

  it('omits an untouched password when saving other settings', async () => {
    await edit()
    const payload = await submit()
    expect(payload).not.toHaveProperty('username')
    expect(payload).not.toHaveProperty('password')
  })

  it('keeps trimming a replacement password', async () => {
    await edit()
    await wrapper.get('#edit-proxy-form input[type="password"]').setValue(' new-password ')
    expect((await submit()).password).toBe('new-password')
  })
})
