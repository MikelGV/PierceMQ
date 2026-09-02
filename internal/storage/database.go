package storage

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/MikelGV/PierceMQ/internal/config"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type DbStore struct {
	Conn *sql.DB
}

func Conn() (*DbStore, error) {
	db, err := sql.Open("pgx", config.Env.DB_URL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error pinging the db: %s", err)
	}

	if err := db.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "Error pinging the db: %s", err)
	}

	return &DbStore{
		Conn: db,
	}, nil
}
