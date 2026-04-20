package config

import (
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration.
type Config struct {
	DB        DBConfig `yaml:"db"`
	LogLevel  string   `yaml:"log_level,omitempty"`
	Transport string   `yaml:"transport"`
	Addr      string   `yaml:"addr"`
}

// DBConfig holds SQLite connection settings.
type DBConfig struct {
	Path          string `yaml:"path,omitempty"`
	BusyTimeoutMs int    `yaml:"busy_timeout_ms,omitempty"`
}

// Resolve loads configuration from the first available source.
// Priority: explicit path → $CHRONOLOG_CONFIG → ./chronolog.yaml → ~/.chronolog/chronolog.yaml → defaults.
func Resolve(explicit string) (*Config, error) {
	paths := []string{explicit}
	if v := os.Getenv("CHRONOLOG_CONFIG"); v != "" {
		paths = append(paths, v)
	}
	paths = append(paths, "chronolog.yaml")
	if home, err := os.UserHomeDir(); err == nil {
		paths = append(paths, filepath.Join(home, ".chronolog", "chronolog.yaml"))
	}

	for _, p := range paths {
		if p == "" {
			continue
		}
		cfg, err := load(p)
		if err == nil {
			applyDefaults(cfg)
			return cfg, nil
		}
		if !os.IsNotExist(err) {
			return nil, err
		}
	}

	cfg := &Config{}
	applyDefaults(cfg)
	return cfg, nil
}

func load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func applyDefaults(cfg *Config) {
	if cfg.DB.Path == "" {
		cfg.DB.Path = envOr("CHRONOLOG_DB", "chronolog.db")
	}
	if cfg.Transport == "" {
		cfg.Transport = "stdio"
	}
	if cfg.Addr == "" {
		cfg.Addr = ":8082"
	}
	if cfg.DB.BusyTimeoutMs == 0 {
		cfg.DB.BusyTimeoutMs = 5000
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
