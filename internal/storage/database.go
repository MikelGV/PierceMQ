package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// DbStore is a single pooled handle. Open one per PgBouncer pool
// (write -> piercemq, read -> piercemq_ro); see Stores below.
type DbStore struct {
	Conn *sql.DB
}

// Stores holds the write (primary) and read (replica) handles.
// PgBouncer runs in transaction mode, so handles must NOT rely on
// connection-level state: no long-lived Prepare, no SET, no temp tables,
// no LISTEN/NOTIFY, no advisory locks. Plain Query/Exec via database/sql
// checks a connection out per call and is safe.
type Stores struct {
	Write *DbStore
	Read  *DbStore
}

// openConn dials one DSN, fails fast on ping, and sizes the database/sql
// pool to fit inside its PgBouncer pool (default_pool_size 25 per dbname).
func openConn(ctx context.Context, dsn string, maxOpen, maxIdle int) (*DbStore, error) {
	if dsn == "" {
		return nil, fmt.Errorf("storage: empty DSN")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("storage: open %s: %w", redact(dsn), err)
	}

	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(time.Hour)
	db.SetConnMaxIdleTime(30 * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("storage: ping %s: %w", redact(dsn), err)
	}

	return &DbStore{Conn: db}, nil
}

// Connect opens write + read handles. Write pool is sized for transactional
// API traffic; read pool is smaller (replica offload, lag-tolerant queries).
func Connect(ctx context.Context, writeDSN, readDSN string) (*Stores, error) {
	write, err := openConn(ctx, writeDSN, 20, 5)
	if err != nil {
		return nil, fmt.Errorf("storage: write pool: %w", err)
	}

	read, err := openConn(ctx, readDSN, 10, 3)
	if err != nil {
		write.Conn.Close()
		return nil, fmt.Errorf("storage: read pool: %w", err)
	}

	return &Stores{Write: write, Read: read}, nil
}

// Close releases both pools.
func (s *Stores) Close() error {
	var first error
	if s == nil {
		return nil
	}
	if s.Write != nil {
		if err := s.Write.Conn.Close(); err != nil && first == nil {
			first = err
		}
	}
	if s.Read != nil {
		if err := s.Read.Conn.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// redact strips credentials for error messages.
func redact(dsn string) string {
	for i := 0; i < len(dsn); i++ {
		if dsn[i] == '@' {
			// keep scheme://***@rest
			if j := index(dsn, "://"); j >= 0 {
				return dsn[:j+3] + "***" + dsn[i:]
			}
			return "***" + dsn[i:]
		}
	}
	return dsn
}

func index(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
