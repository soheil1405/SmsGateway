package config

import (
	"fmt"
	"net/url"
	"os"
)

type Config struct {
	Addr     string
	Database Database
}

type Database struct {
	Host     string
	Port     string
	User     string
	Password string
	Name     string
	SSLMode  string
	URL      string // optional; if set, overrides the fields above
}

func (d Database) DSN() string {
	if d.URL != "" {
		return d.URL
	}

	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(d.User, d.Password),
		Host:   fmt.Sprintf("%s:%s", d.Host, d.Port),
		Path:   "/" + d.Name,
	}
	q := u.Query()
	q.Set("sslmode", d.SSLMode)
	u.RawQuery = q.Encode()
	return u.String()
}

func Load() Config {
	return Config{
		Addr: env("ADDR", ":8080"),
		Database: Database{
			URL:      os.Getenv("DATABASE_URL"),
			Host:     env("DB_HOST", "localhost"),
			Port:     env("DB_PORT", "5432"),
			User:     env("DB_USER", "postgres"),
			Password: env("DB_PASSWORD", "postgres"),
			Name:     env("DB_NAME", "arvan"),
			SSLMode:  env("DB_SSLMODE", "disable"),
		},
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
