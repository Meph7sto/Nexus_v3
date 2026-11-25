package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEntryNodeOptionalSharedTaskOwner(t *testing.T) {
	c := EntryNodeConfig{ClusterEnabled: true, ID: "control-1", Role: EntryRoleControlOnly}
	require.NoError(t, c.validate())
	require.True(t, c.AllowsControl())
	require.False(t, c.DesignatedTaskOwner())

	c.SharedTaskOwnerID = "control-1"
	require.NoError(t, c.validate())
	require.True(t, c.DesignatedTaskOwner())

	c.Role = EntryRoleGatewayOnly
	require.ErrorContains(t, c.validate(), "gateway_only cannot own shared tasks")
	c.SharedTaskOwnerID = ""
	require.NoError(t, c.validate())
	require.False(t, c.AllowsControl())

	c.SharedTaskOwnerID = "not a valid ID"
	require.ErrorContains(t, c.validate(), "must be valid identifiers")
}
