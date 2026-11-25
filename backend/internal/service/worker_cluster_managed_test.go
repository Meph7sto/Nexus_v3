package service

import (
	"errors"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

const managedWorkerTestToken = "test-worker-managed-token-0123456789"

type managedWorkerTestEncryptor struct{ writable bool }

func (e managedWorkerTestEncryptor) Encrypt(plaintext string) (string, error) {
	if e.writable && plaintext == managedWorkerTestToken {
		return "test-ciphertext", nil
	}
	return "", errors.New("canary plaintext encryptor error")
}

func (managedWorkerTestEncryptor) Decrypt(ciphertext string) (string, error) {
	if ciphertext == "test-ciphertext" {
		return managedWorkerTestToken, nil
	}
	return "", errors.New("canary ciphertext encryptor error")
}

func TestManagedWorkerCredentialsPreferCiphertextAndFailClosed(t *testing.T) {
	cfg := config.WorkerPoolConfig{AllowPrivateH2C: true, Bindings: []config.WorkerBindingConfig{{
		Ref: "legacy", NodeID: "worker", URL: "http://127.0.0.1:9000", SharedTokenFile: "/run/worker-token",
	}}}
	store := &recoveryStore{snapshot: snapshotFromBootstrap(cfg)}
	cluster, err := NewWorkerClusterService(store, &recoveryCoord{}, &recoveryRuntime{}, &config.Config{
		WorkerPool: cfg, EntryNode: config.EntryNodeConfig{ClusterEnabled: true, ID: "control", Role: "control_only"},
		Totp: config.TotpConfig{EncryptionKeyConfigured: true},
	}, nil, managedWorkerTestEncryptor{})
	require.NoError(t, err)
	node := WorkerNode{NodeID: "worker", URL: "http://127.0.0.1:9000", UseH2C: true, SecretRef: "legacy"}
	legacy, err := cluster.resolveWorker(node)
	require.NoError(t, err)
	require.Empty(t, legacy.SharedToken)
	require.Equal(t, "/run/worker-token", legacy.SharedTokenFile)
	node.EncryptedToken = "test-ciphertext"
	managed, err := cluster.resolveWorker(node)
	require.NoError(t, err)
	require.Equal(t, managedWorkerTestToken, managed.SharedToken)
	require.Empty(t, managed.SharedTokenFile)
	node.EncryptedToken = "corrupted"
	_, err = cluster.resolveWorker(node)
	require.ErrorIs(t, err, ErrWorkerClusterUnavailable, "do not silently switch to the legacy binding")
	require.NotContains(t, err.Error(), "canary")
}

func TestManagedWorkerRequiresFixedKeyAndHidesEncryptionErrors(t *testing.T) {
	cfg := config.WorkerPoolConfig{AllowPrivateH2C: true}
	store := &recoveryStore{snapshot: snapshotFromBootstrap(cfg)}
	cluster, err := NewWorkerClusterService(store, &recoveryCoord{}, &recoveryRuntime{}, &config.Config{
		WorkerPool: cfg, EntryNode: config.EntryNodeConfig{ClusterEnabled: true, ID: "control", Role: "control_only"},
	}, nil, managedWorkerTestEncryptor{})
	require.NoError(t, err)
	node := WorkerNode{NodeID: "worker", Name: "worker", URL: "http://127.0.0.1:9000", UseH2C: true, Weight: 1}
	_, err = cluster.UpsertWorker(t.Context(), node, 1, managedWorkerTestToken)
	require.ErrorIs(t, err, ErrWorkerTokenKeyNotConfigured)
	cluster.keyConfigured = true
	_, err = cluster.UpsertWorker(t.Context(), node, 1, managedWorkerTestToken)
	require.ErrorIs(t, err, ErrWorkerClusterUnavailable)
	require.NotContains(t, err.Error(), "canary")
	require.Empty(t, store.snapshot.Workers)
	_, err = cluster.UpsertWorker(t.Context(), node, 1, "")
	require.ErrorIs(t, err, ErrWorkerClusterInvalid)
}

func TestManagedWorkerSaveChecksDNSOnlyWhenActivatingOrChangingOrigin(t *testing.T) {
	const (
		offline         = "http://unconfigured-worker.invalid."
		private         = "http://localhost:9090"
		invalidDNS      = "Worker hostname must resolve only to private addresses"
		invalidEndpoint = "Worker URL or transport is not allowed"
	)
	for _, tc := range []struct {
		name, previousURL, url string
		wasEnabled, enabled    bool
		disallowH2C            bool
		wantError              string
	}{
		{name: "pre-register disabled", url: offline},
		{name: "edit disabled to unresolved origin", previousURL: private, url: offline},
		{name: "disable even if DNS disappeared", previousURL: offline, wasEnabled: true, url: offline},
		{name: "update weight and note even if DNS disappeared", previousURL: offline, wasEnabled: true, url: offline, enabled: true},
		{name: "equivalent origin needs no lookup", previousURL: offline, wasEnabled: true, url: " http://UNCONFIGURED-WORKER.INVALID.:080/ ", enabled: true},
		{name: "new enabled node needs DNS", url: offline, enabled: true, wantError: invalidDNS},
		{name: "new enabled node with private DNS", url: private, enabled: true},
		{name: "re-enable needs DNS", previousURL: offline, url: offline, enabled: true, wantError: invalidDNS},
		{name: "re-enable with private DNS", previousURL: private, url: private, enabled: true},
		{name: "enabled origin change needs DNS", previousURL: private, wasEnabled: true, url: offline, enabled: true, wantError: invalidDNS},
		{name: "enabled origin change to private DNS", previousURL: offline, wasEnabled: true, url: private, enabled: true},
		{name: "effective port change needs DNS", previousURL: offline, wasEnabled: true, url: offline + ":90", enabled: true, wantError: invalidDNS},
		{name: "disabled public literal", url: "http://8.8.8.8", wantError: invalidEndpoint},
		{name: "disabled invalid port", url: offline + ":65536", wantError: invalidEndpoint},
		{name: "disabled URL with path", url: offline + "/worker", wantError: invalidEndpoint},
		{name: "disabled HTTPS with h2c selected", url: "https://localhost:9090", wantError: invalidEndpoint},
		{name: "disabled h2c without local permission", url: private, disallowH2C: true, wantError: invalidEndpoint},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.WorkerPoolConfig{AllowPrivateH2C: !tc.disallowH2C}
			initial := snapshotFromBootstrap(cfg)
			node := WorkerNode{NodeID: "worker", Name: "Worker", URL: tc.url, UseH2C: true,
				Weight: 2, Note: "updated metadata", Enabled: tc.enabled}
			sharedToken := managedWorkerTestToken
			if tc.previousURL != "" {
				initial.Workers = []WorkerNode{{NodeID: node.NodeID, Name: node.Name, URL: tc.previousURL,
					UseH2C: true, Weight: 1, Enabled: tc.wasEnabled, EncryptedToken: "test-ciphertext"}}
				sharedToken = ""
			}
			store := &recoveryStore{snapshot: initial}
			cluster, err := NewWorkerClusterService(store, &recoveryCoord{}, &recoveryRuntime{}, &config.Config{
				WorkerPool: cfg, EntryNode: config.EntryNodeConfig{ClusterEnabled: true, ID: "control", Role: "control_only"},
				Totp: config.TotpConfig{EncryptionKeyConfigured: true},
			}, nil, managedWorkerTestEncryptor{writable: true})
			require.NoError(t, err)
			updated, err := cluster.UpsertWorker(t.Context(), node, initial.Version, sharedToken)
			if tc.wantError != "" {
				require.ErrorIs(t, err, ErrWorkerClusterInvalid)
				require.ErrorContains(t, err, tc.wantError)
				require.Equal(t, initial, store.snapshot, "a rejected save must not change the stored node or version")
				return
			}
			require.NoError(t, err)
			require.Equal(t, initial.Version+1, store.snapshot.Version)
			require.Len(t, updated.Workers, 1)
			require.Equal(t, node.Enabled, updated.Workers[0].Enabled)
			require.Equal(t, node.Weight, updated.Workers[0].Weight)
			require.Equal(t, node.Note, updated.Workers[0].Note)
			require.Equal(t, "test-ciphertext", store.snapshot.Workers[0].EncryptedToken)
		})
	}
}
