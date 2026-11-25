package config

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/spf13/viper"
)

const (
	EntryRoleAll         = "all"
	EntryRoleGatewayOnly = "gateway_only"
	EntryRoleControlOnly = "control_only"
)

type EntryNodeConfig struct {
	ShutdownTimeout   time.Duration `mapstructure:"shutdown_timeout"`
	ClusterEnabled    bool          `mapstructure:"cluster_enabled"`
	ID                string        `mapstructure:"id"`
	Name              string        `mapstructure:"name"`
	Role              string        `mapstructure:"role"`
	SharedTaskOwnerID string        `mapstructure:"shared_task_owner_id"`
}

func (c EntryNodeConfig) AllowsGateway() bool { return c.Role != EntryRoleControlOnly }

func (c EntryNodeConfig) AllowsControl() bool { return c.Role != EntryRoleGatewayOnly }

func (c EntryNodeConfig) DesignatedTaskOwner() bool {
	return !c.ClusterEnabled || (c.AllowsControl() && c.ID != "" && c.ID == c.SharedTaskOwnerID)
}

var safeEntryID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$`)

// EntryRoleForSetup reads just the role before the normal application config exists.
func EntryRoleForSetup() string {
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	configureConfigSource(v.SetConfigFile, v.AddConfigPath)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	_ = v.ReadInConfig()
	return strings.TrimSpace(v.GetString("entry_node.role"))
}

func (c *EntryNodeConfig) validate() error {
	if c.ShutdownTimeout <= 0 {
		if c.ShutdownTimeout < 0 {
			return fmt.Errorf("entry_node.shutdown_timeout must be positive")
		}
		c.ShutdownTimeout = 2 * time.Minute
	}
	c.ID = strings.TrimSpace(c.ID)
	c.Name = strings.TrimSpace(c.Name)
	c.Role = strings.TrimSpace(c.Role)
	c.SharedTaskOwnerID = strings.TrimSpace(c.SharedTaskOwnerID)
	if c.Role == "" {
		c.Role = EntryRoleAll
	}
	switch c.Role {
	case EntryRoleAll, EntryRoleGatewayOnly, EntryRoleControlOnly:
	default:
		return fmt.Errorf("entry_node.role must be all, gateway_only, or control_only")
	}
	if !c.ClusterEnabled {
		if c.Role != EntryRoleAll {
			return fmt.Errorf("entry_node.role requires cluster_enabled")
		}
		return nil
	}
	if !safeEntryID.MatchString(c.ID) || (c.SharedTaskOwnerID != "" && !safeEntryID.MatchString(c.SharedTaskOwnerID)) {
		return fmt.Errorf("entry_node.id and shared_task_owner_id must be valid identifiers")
	}
	if len(c.Name) > 128 {
		return fmt.Errorf("entry_node.name must not exceed 128 bytes")
	}
	if c.Name == "" {
		c.Name = c.ID
	}
	if c.Role == EntryRoleGatewayOnly && c.ID == c.SharedTaskOwnerID {
		return fmt.Errorf("gateway_only cannot own shared tasks")
	}
	return nil
}
