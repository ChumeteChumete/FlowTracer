package healthcheck

import (
    "context"
    "database/sql"
    "time"
)

type PostgresChecker struct {
    db      *sql.DB
    timeout time.Duration
}

func NewPostgresChecker(db *sql.DB, timeout time.Duration) *PostgresChecker {
    return &PostgresChecker{db: db, timeout: timeout}
}

func (p *PostgresChecker) Name() string {
    return "postgres"
}

func (p *PostgresChecker) Check(ctx context.Context) error {
    ctx, cancel := context.WithTimeout(ctx, p.timeout)
    defer cancel()
    
    return p.db.PingContext(ctx)
}