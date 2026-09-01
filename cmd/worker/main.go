package worker

import (
	"context"
	"fmt"
	"os"

	"github.com/MikelGV/PierceMQ/internal/worker"
)

func main() {
	ctx := context.Background()
	var w *worker.Worker

	if err := w.Run(ctx, os.Stdout, os.Getenv); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

}
