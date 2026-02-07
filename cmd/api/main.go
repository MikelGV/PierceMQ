package main

import (
	"context"
	"fmt"
	"os"

	"github.com/MikelGV/PierceMQ/internal/api"
)

func main() {
	ctx := context.Background()

	if err := api.Run(ctx, os.Stdout, os.Getenv); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}
