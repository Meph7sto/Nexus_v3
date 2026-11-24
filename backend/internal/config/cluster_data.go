package config

// ClusterDataConfig contains operator-authorized Data preflight targets. An
// administrative request selects a profile ID, never a URL or a secret source.
type ClusterDataConfig struct {
	Profiles []ClusterDataProfile `mapstructure:"profiles"`
}

type ClusterDataProfile struct {
	ID                  string              `mapstructure:"id"`
	Label               string              `mapstructure:"label"`
	Mode                string              `mapstructure:"mode"`
	Postgres            DatabaseConfig      `mapstructure:"postgres"`
	Redis               RedisConfig         `mapstructure:"redis"`
	PostgresPasswordEnv string              `mapstructure:"postgres_password_env"`
	RedisPasswordEnv    string              `mapstructure:"redis_password_env"`
	Sentinel            ClusterDataSentinel `mapstructure:"sentinel"`
}

type ClusterDataSentinel struct {
	Addresses        []string `mapstructure:"addresses"`
	MasterName       string   `mapstructure:"master_name"`
	AllowedPrimaries []string `mapstructure:"allowed_primaries"`
	Username         string   `mapstructure:"username"`
	PasswordEnv      string   `mapstructure:"password_env"`
	EnableTLS        bool     `mapstructure:"enable_tls"`
}
