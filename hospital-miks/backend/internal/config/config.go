package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds all application configuration.
type Config struct {
	Server   ServerConfig
	Database DatabaseConfig
	JWT      JWTConfig
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Host string
	Port int
}

// DatabaseConfig holds PostgreSQL/ASQL connection settings.
type DatabaseConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
}

// JWTConfig holds JWT authentication settings.
type JWTConfig struct {
	Secret     string
	ExpireHours int
}

// DSN builds the PostgreSQL connection string.
func (d DatabaseConfig) DSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		d.User, d.Password, d.Host, d.Port, d.DBName, d.SSLMode,
	)
}

// Load reads configuration from environment variables with defaults.
func Load() Config {
	return Config{
		Server: ServerConfig{
			Host: envOrDefault("SERVER_HOST", "0.0.0.0"),
			Port: envOrDefaultInt("SERVER_PORT", 8080),
		},
		Database: DatabaseConfig{
			Host:     envOrDefault("DB_HOST", "127.0.0.1"),
			Port:     envOrDefaultInt("DB_PORT", 5432),
			User:     envOrDefault("DB_USER", "asql"),
			Password: envOrDefault("DB_PASSWORD", "asql"),
			DBName:   envOrDefault("DB_NAME", "hospital_miks"),
			SSLMode:  envOrDefault("DB_SSLMODE", "disable"),
		},
		JWT: JWTConfig{
			Secret:     envOrDefault("JWT_SECRET", "hospital-miks-secret-change-me"),
			ExpireHours: envOrDefaultInt("JWT_EXPIRE_HOURS", 24),
		},
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envOrDefaultInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
