package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect creates a pgxpool.Pool from the given DSN.
// Returns an error if the connection cannot be established within 10 seconds.
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("connect: parse dsn: %w", err)
	}

	poolCfg.MinConns = 1
	poolCfg.MaxConns = 4
	poolCfg.ConnConfig.ConnectTimeout = 10 * time.Second
	if poolCfg.ConnConfig.RuntimeParams == nil {
		poolCfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	poolCfg.ConnConfig.RuntimeParams["application_name"] = "strata"

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("connect: create pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect: ping: %w", err)
	}

	return pool, nil
}

// Fingerprint returns a non-reversible SHA256 fingerprint of the DSN host+port.
// Used in the SMIF document source.host_fingerprint field.
// Does not include credentials.
func Fingerprint(dsn string) (string, error) {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return "", fmt.Errorf("fingerprint: parse dsn: %w", err)
	}

	input := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	hash := sha256.Sum256([]byte(input))
	encoded := hex.EncodeToString(hash[:])

	return encoded[:16], nil
}
