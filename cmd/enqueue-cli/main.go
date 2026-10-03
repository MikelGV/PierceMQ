// Command enqueue-cli creates and inspects jobs via the HTTP API (§4:
// jobs can be created via the client application or the CLI). It is a thin
// wrapper over pkg/client: every subcommand maps to one API call.
//
// Env: PMQ_URL (default http://localhost:8000), PMQ_TOKEN (or --token).
// Auth-bearing subcommands need a JWT (register/login) or API key.
//
// Usage:
//
//	enqueue-cli register --name n --email e --password p
//	enqueue-cli login --email e --password p
//	enqueue-cli submit --type email --payload '{"to":"a","from":"b"}' [--queue email-high] [--priority 1] [--max-retry 3] [--idempotency k] [--scheduled-at RFC3339]
//	enqueue-cli get --id <uuid>
//	enqueue-cli events --id <uuid>
//	enqueue-cli list [--status failed] [--limit 50] [--offset 0]
//	enqueue-cli cancel --id <uuid>
//	enqueue-cli retry --id <uuid>
//	enqueue-cli stats
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/MikelGV/PierceMQ/pkg/client"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "enqueue-cli:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	args, token := extractTokenFlag(args)
	if token == "" {
		token = os.Getenv("PMQ_TOKEN")
	}
	base := os.Getenv("PMQ_URL")
	if base == "" {
		base = "http://localhost:8000"
	}
	c := &client.Client{BaseURL: base, Token: token}
	if len(args) == 0 {
		return fmt.Errorf("usage: enqueue-cli [--token T] <register|login|submit|get|events|list|cancel|retry|stats> [flags]")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	cmd, rest := args[0], args[1:]
	switch cmd {
	case "register":
		fs := flag.NewFlagSet("register", flag.ContinueOnError)
		name := fs.String("name", "", "user name")
		email := fs.String("email", "", "email")
		password := fs.String("password", "", "password (min 8 chars)")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if *name == "" || *email == "" || *password == "" {
			return fmt.Errorf("register requires --name, --email, --password")
		}
		out, err := c.Register(ctx, *name, *email, *password)
		if err != nil {
			return err
		}
		return printJSON(out)
	case "login":
		fs := flag.NewFlagSet("login", flag.ContinueOnError)
		email := fs.String("email", "", "email")
		password := fs.String("password", "", "password")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if *email == "" || *password == "" {
			return fmt.Errorf("login requires --email, --password")
		}
		token, err := c.Login(ctx, *email, *password)
		if err != nil {
			return err
		}
		fmt.Println(token)
		return nil
	case "submit":
		fs := flag.NewFlagSet("submit", flag.ContinueOnError)
		jobType := fs.String("type", "", "job type: email, file, file_processing, exec, exec_processing")
		queue := fs.String("queue", "", "queue name, e.g. email-high (default <type>-high)")
		payload := fs.String("payload", "", "JSON object payload")
		payloadFile := fs.String("payload-file", "", "read JSON payload from file")
		priority := fs.Int("priority", 0, "priority (>0 selects high stream)")
		maxRetry := fs.Int("max-retry", 0, "max retries (0 = server default 3)")
		idempotency := fs.String("idempotency", "", "idempotency key")
		scheduledAt := fs.String("scheduled-at", "", "RFC3339 future time")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if *jobType == "" {
			return fmt.Errorf("submit requires --type")
		}
		raw := *payload
		if *payloadFile != "" {
			b, err := os.ReadFile(*payloadFile)
			if err != nil {
				return fmt.Errorf("read payload file: %w", err)
			}
			raw = string(b)
		}
		if strings.TrimSpace(raw) == "" {
			return fmt.Errorf("submit requires --payload or --payload-file")
		}
		var obj map[string]any
		if err := json.Unmarshal([]byte(raw), &obj); err != nil {
			return fmt.Errorf("payload must be a JSON object: %w", err)
		}
		out, err := c.Enqueue(ctx, client.EnqueueRequest{
			Type:           *jobType,
			QueueName:      *queue,
			Payload:        obj,
			Priority:       int16(*priority),
			MaxRetry:       int16(*maxRetry),
			IdempotencyKey: *idempotency,
			ScheduledAt:    *scheduledAt,
		})
		if err != nil {
			return err
		}
		return printJSON(out)
	case "get":
		fs := flag.NewFlagSet("get", flag.ContinueOnError)
		id := fs.String("id", "", "job id")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if *id == "" {
			return fmt.Errorf("get requires --id")
		}
		out, err := c.GetJob(ctx, *id)
		if err != nil {
			return err
		}
		return printJSON(out)
	case "events":
		fs := flag.NewFlagSet("events", flag.ContinueOnError)
		id := fs.String("id", "", "job id")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if *id == "" {
			return fmt.Errorf("events requires --id")
		}
		out, err := c.GetJobEvents(ctx, *id)
		if err != nil {
			return err
		}
		return printJSON(out)
	case "list":
		fs := flag.NewFlagSet("list", flag.ContinueOnError)
		status := fs.String("status", "", "status filter")
		limit := fs.Int("limit", 0, "page size (server default 50, max 100)")
		offset := fs.Int("offset", 0, "page offset")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		out, err := c.ListJobs(ctx, *status, *limit, *offset)
		if err != nil {
			return err
		}
		return printJSON(out)
	case "cancel":
		fs := flag.NewFlagSet("cancel", flag.ContinueOnError)
		id := fs.String("id", "", "job id")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if *id == "" {
			return fmt.Errorf("cancel requires --id")
		}
		out, err := c.Cancel(ctx, *id)
		if err != nil {
			return err
		}
		return printJSON(out)
	case "retry":
		fs := flag.NewFlagSet("retry", flag.ContinueOnError)
		id := fs.String("id", "", "job id")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if *id == "" {
			return fmt.Errorf("retry requires --id")
		}
		out, err := c.Retry(ctx, *id)
		if err != nil {
			return err
		}
		return printJSON(out)
	case "stats":
		out, err := c.Stats(ctx)
		if err != nil {
			return err
		}
		return printJSON(out)
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
}

// extractTokenFlag pulls a global --token value (either --token T or
// --token=T) out of args so subcommand FlagSets never see it.
func extractTokenFlag(args []string) ([]string, string) {
	var kept []string
	var token string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if v, ok := strings.CutPrefix(a, "--token="); ok {
			token = v
			continue
		}
		if a == "--token" && i+1 < len(args) {
			token = args[i+1]
			i++
			continue
		}
		kept = append(kept, a)
	}
	return kept, token
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
