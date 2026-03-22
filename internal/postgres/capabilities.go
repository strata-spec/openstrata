package postgres

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

var tableSampleSupportByPool sync.Map // map[*pgxpool.Pool]bool

func cachePoolCapabilities(ctx context.Context, pool *pgxpool.Pool) {
	if pool == nil {
		return
	}

	const sql = "SELECT current_setting('server_version_num')::int >= 90500"
	supports := false
	if err := pool.QueryRow(ctx, sql).Scan(&supports); err != nil {
		supports = false
	}
	tableSampleSupportByPool.Store(pool, supports)
}

func poolSupportsTableSample(pool *pgxpool.Pool) bool {
	if pool == nil {
		return false
	}
	if v, ok := tableSampleSupportByPool.Load(pool); ok {
		if b, okBool := v.(bool); okBool {
			return b
		}
	}
	return false
}
