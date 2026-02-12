package config

import (
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

// Config represents the top-level YAML configuration.
type Config struct {
	Connection    Connection  `yaml:"connection"`
	Roots         []Root      `yaml:"roots"`
	ExcludeTables []string    `yaml:"exclude_tables"`
	Schemas       []string    `yaml:"schemas"`
	Output        string      `yaml:"output"`
}

// Connection holds database connection parameters.
type Connection struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	SSLMode  string `yaml:"sslmode"`
}

// Root defines a root table with an optional WHERE clause.
type Root struct {
	Table string `yaml:"table"`
	Where string `yaml:"where"`
}

// DSN builds a PostgreSQL connection string.
func (c *Connection) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		c.Host, c.Port, c.Database, c.User, c.Password, c.SSLMode,
	)
}

// Load reads and parses a YAML config file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	cfg.applyEnv()

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &cfg, nil
}

// applyEnv fills in empty Connection fields from environment variables.
// YAML values take precedence; env vars are used only as fallback.
func (c *Config) applyEnv() {
	conn := &c.Connection
	if conn.Host == "" {
		conn.Host = envOr("PGHOST", "POSTGRES_HOST", "")
	}
	if conn.Port == 0 {
		if s := envOr("PGPORT", "POSTGRES_PORT", ""); s != "" {
			if p, err := strconv.Atoi(s); err == nil {
				conn.Port = p
			}
		}
	}
	if conn.Database == "" {
		conn.Database = envOr("PGDATABASE", "POSTGRES_DB", "")
	}
	if conn.User == "" {
		conn.User = envOr("PGUSER", "POSTGRES_USER", "")
	}
	if conn.Password == "" {
		conn.Password = envOr("PGPASSWORD", "POSTGRES_PASSWORD", "")
	}
	if conn.SSLMode == "" {
		conn.SSLMode = envOr("PGSSLMODE", "", "")
	}
}

// envOr returns the first non-empty value from the given env var names, or fallback.
func envOr(names ...string) string {
	for _, n := range names {
		if n == "" {
			continue
		}
		if v := os.Getenv(n); v != "" {
			return v
		}
	}
	return ""
}

// validate checks connection and schema defaults (sufficient for analyze).
func (c *Config) validate() error {
	if c.Connection.Host == "" {
		return fmt.Errorf("connection.host is required")
	}
	if c.Connection.Port == 0 {
		c.Connection.Port = 5432
	}
	if c.Connection.Database == "" {
		return fmt.Errorf("connection.database is required")
	}
	if c.Connection.User == "" {
		return fmt.Errorf("connection.user is required")
	}
	if c.Connection.SSLMode == "" {
		c.Connection.SSLMode = "disable"
	}
	if len(c.Schemas) == 0 {
		c.Schemas = []string{"public"}
	}
	return nil
}

// ValidateForExtract checks additional fields required for extraction.
func (c *Config) ValidateForExtract() error {
	if len(c.Roots) == 0 {
		return fmt.Errorf("at least one root table must be specified in config")
	}
	for i, r := range c.Roots {
		if r.Table == "" {
			return fmt.Errorf("roots[%d].table is required", i)
		}
	}
	return nil
}

// ExcludeSet returns a set of excluded table names for O(1) lookup.
func (c *Config) ExcludeSet() map[string]bool {
	set := make(map[string]bool, len(c.ExcludeTables))
	for _, t := range c.ExcludeTables {
		set[t] = true
	}
	return set
}
