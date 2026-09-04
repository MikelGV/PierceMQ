package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/MikelGV/PierceMQ/internal/api"
	"github.com/MikelGV/PierceMQ/internal/config"
	"github.com/MikelGV/PierceMQ/internal/storage"
)

func main() {
	ctx := context.Background()

	// `api migrate up|down|version|force <v>` runs embedded schema
	// migrations against the WRITE pool and exits. Executed by the
	// db-migrate one-shot in docker-compose.yaml, never by replicas.
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		if err := runMigrate(ctx, os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "migrate: %s\n", err)
			os.Exit(1)
		}
		return
	}

	if err := api.Run(ctx, os.Stdout, os.Getenv); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func runMigrate(ctx context.Context, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: api migrate up|down|version|force <v>")
	}
	dsn := config.Env.DB_URL

	switch args[0] {
	case "up":
		if err := storage.MigrateUp(ctx, dsn); err != nil {
			return err
		}
		fmt.Println("migrate: up to date")
		return nil
	case "down":
		if err := storage.MigrateDown(ctx, dsn); err != nil {
			return err
		}
		fmt.Println("migrate: rolled back one step")
		return nil
	case "version":
		v, dirty, err := storage.MigrateVersion(ctx, dsn)
		if err != nil {
			return err
		}
		fmt.Printf("migrate: version %d dirty=%v\n", v, dirty)
		return nil
	case "force":
		if len(args) < 2 {
			return fmt.Errorf("usage: api migrate force <v>")
		}
		v, err := strconv.Atoi(args[1])
		if err != nil {
			return fmt.Errorf("invalid version %q: %w", args[1], err)
		}
		return storage.MigrateForce(ctx, dsn, v)
	default:
		return fmt.Errorf("unknown migrate command %q", args[0])
	}
}
