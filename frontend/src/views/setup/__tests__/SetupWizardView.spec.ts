import { flushPromises, mount } from '@vue/test-utils'
import { describe, expect, it, vi } from 'vitest'
import Select from '@/components/common/Select.vue'
import SetupWizardView from '../SetupWizardView.vue'

const api = vi.hoisted(() => ({
  testDatabase: vi.fn().mockResolvedValue(undefined),
  testRedis: vi.fn().mockResolvedValue(undefined),
  install: vi.fn().mockRejectedValue(new Error('test completes after submission'))
}))

vi.mock('@/api/setup', () => api)
vi.mock('@/api/client', () => ({ buildGatewayUrl: (path: string) => path }))
vi.mock('vue-i18n', () => ({ useI18n: () => ({ t: (key: string) => key }) }))

describe('Setup data topology', () => {
  it('probes and installs with the same Sentinel settings and no pinned primary', async () => {
    const wizard = mount(SetupWizardView)
    const button = (text: string) => wizard.findAll('button').find(candidate => candidate.text() === text)!
    const next = async () => button('common.next').trigger('click')
    try {
      const topology = wizard.findComponent(Select)
      expect(topology.props('options')).toEqual(expect.arrayContaining([
        { value: 'single', label: 'setup.topology.single' },
        { value: 'ha', label: 'setup.topology.ha' },
        { value: 'managed', label: 'setup.topology.managed' }
      ]))
      topology.vm.$emit('update:modelValue', 'ha')
      await flushPromises()
      await button('setup.status.testConnection').trigger('click')
      await flushPromises()
      await next()
      await wizard.get('#setup-sentinel-addresses').setValue('data-1.internal:26379, data-2.internal:26379')
      await wizard.get('#setup-sentinel-primaries').setValue('data-1.internal:6379\ndata-2.internal:6379')
      await wizard.get('#setup-sentinel-password').setValue('sentinel-fixture-only')
      await button('setup.status.testConnection').trigger('click')
      await flushPromises()
      const redis = api.testRedis.mock.calls[0][0]
      expect(redis).toEqual(expect.objectContaining({
        host: '', port: 0,
        sentinel: expect.objectContaining({
          master_name: 'nexus',
          addresses: ['data-1.internal:26379', 'data-2.internal:26379'],
          allowed_primaries: ['data-1.internal:6379', 'data-2.internal:6379'],
          password: 'sentinel-fixture-only'
        })
      }))
      await next()
      await wizard.get('input[type="email"]').setValue('qa@example.test')
      for (const field of wizard.findAll('input[type="password"]')) {
        await field.setValue('fixture-demo-password')
      }
      await next()
      await button('setup.status.completeInstallation').trigger('click')
      await flushPromises()
      expect(api.install).toHaveBeenCalledWith(expect.objectContaining({ redis }))
      expect(wizard.text()).not.toContain('sentinel-fixture-only')
    } finally {
      wizard.unmount()
    }
  })
})
