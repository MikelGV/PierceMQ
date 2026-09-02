package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MikelGV/PierceMQ/internal/broker"
	"github.com/MikelGV/PierceMQ/internal/dispatcher"
	"github.com/MikelGV/PierceMQ/internal/queue"
	"github.com/MikelGV/PierceMQ/internal/task"
	"github.com/redis/go-redis/v9"
)

type Worker struct {
	ID         int
	JobChannel chan *task.Job
	Worker     chan *Worker
	PoolID     string
}

// poolConfig collapsed high/low into one OS process per type with internal priority queue.
type poolConfig struct {
	Type       string
	HighStream string
	LowStream  string
	HighGroup  string
	LowGroup   string
}

var poolConfigs = map[string]poolConfig{
	"email": {
		Type:       "email",
		HighStream: queue.EmailHighStream,
		LowStream:  queue.EmailLowStream,
		HighGroup:  queue.EmailGroupHigh,
		LowGroup:   queue.EmailGroupLow,
	},
	"file_processing": {
		Type:       "file_processing",
		HighStream: queue.FileHighStream,
		LowStream:  queue.FileLowStream,
		HighGroup:  queue.FileGroupHigh,
		LowGroup:   queue.FileGroupLow,
	},
	"exec_processing": {
		Type:       "exec_processing",
		HighStream: queue.ExecHighStream,
		LowStream:  queue.ExecLowStream,
		HighGroup:  queue.ExecGroupHigh,
		LowGroup:   queue.ExecGroupLow,
	},
}

var poolOrder = []string{"email", "file_processing", "exec_processing"}

/**
 * Create a new worker pool
**/
func (w *Worker) NewWorker(id int, workerpool chan *Worker, conn *redis.Client) (*Worker, error) {
	if conn == nil {
		return nil, fmt.Errorf("worker %d: nil redis connection", id)
	}
	if err := conn.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("worker %d: lost connection to redis: %w", id, err)
	}
	return &Worker{
		ID:         id,
		JobChannel: make(chan *task.Job),
		Worker:     workerpool,
	}, nil
}

/**
 * Runs the worker main loop, here we create the processes for email, binary, and file
 * also we create the pools and we run the claimJobs, processJobs, error handling procedures
 * Also here we handle sending and reciving the heartbeats
 *
 * Dual-mode:
 *  - supervisor (no --pool flag / WORKER_POOL env): forks 3 pool processes via exec.Command
 *    with restart on panic/OOM, isolated Go scheduler per OS process.
 *  - pool (with --pool=<type>): connects to Redis, creates dispatcher + worker goroutines,
 *    delegates claiming to broker.ServeJobs (high/low streams) with internal priority queue,
 *    and heartbeats via SETEX.
**/
func (wo *Worker) Run(ctx context.Context, w io.Writer, getenv func(string) string) error {
	if w == nil {
		w = os.Stdout
	}
	if getenv == nil {
		getenv = os.Getenv
	}
	if pool := poolFromArgs(os.Args, getenv); pool != "" {
		return wo.runPool(ctx, w, getenv, pool)
	}
	return runSupervisor(ctx, w, getenv)
}

func poolFromArgs(args []string, getenv func(string) string) string {
	for _, a := range args {
		if strings.HasPrefix(a, "--pool=") {
			v := strings.TrimPrefix(a, "--pool=")
			if _, ok := poolConfigs[v]; ok {
				return v
			}
		}
	}
	if v := getenv("WORKER_POOL"); v != "" {
		if _, ok := poolConfigs[v]; ok {
			return v
		}
	}
	return ""
}

func dynamicPoolSize(getenv func(string) string) int {
	if v := getenv("POOL_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	n := runtime.GOMAXPROCS(0) * 2
	if n < 4 {
		n = 4
	}
	if n > 32 {
		n = 32
	}
	return n
}

func runSupervisor(ctx context.Context, w io.Writer, getenv func(string) string) error {
	if getenv("WORKER_TEST_NOFORK") == "1" {
		fmt.Fprintln(w, "supervisor: test nofork mode — waiting for context")
		<-ctx.Done()
		return ctx.Err()
	}
	fmt.Fprintln(w, "supervisor: starting 3 pool processes (email, file_processing, exec_processing)")

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type child struct {
		pool string
		cmd  *exec.Cmd
	}
	var mu sync.Mutex
	children := make(map[string]*child)

	startChild := func(pool string) error {
		exe, err := os.Executable()
		if err != nil {
			exe = os.Args[0]
		}
		cmd := exec.CommandContext(ctx, exe, "--pool="+pool)
		cmd.Stdout = w
		cmd.Stderr = w
		cmd.Env = os.Environ()
		cmd.Env = append(cmd.Env, "WORKER_POOL="+pool)
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("supervisor: failed to start pool %s: %w", pool, err)
		}
		mu.Lock()
		children[pool] = &child{pool: pool, cmd: cmd}
		mu.Unlock()
		fmt.Fprintf(w, "supervisor: pool %s started pid=%d\n", pool, cmd.Process.Pid)
		return nil
	}

	for _, pool := range poolOrder {
		if err := startChild(pool); err != nil {
			return err
		}
	}

	errCh := make(chan string, 3)
	for pool, ch := range children {
		pool := pool
		cmd := ch.cmd
		go func() {
			err := cmd.Wait()
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				fmt.Fprintf(w, "supervisor: pool %s pid=%d exited: %v — restarting\n", pool, cmd.Process.Pid, err)
			} else {
				fmt.Fprintf(w, "supervisor: pool %s pid=%d exited cleanly — restarting\n", pool, cmd.Process.Pid)
			}
			errCh <- pool
		}()
	}

	backoff := map[string]time.Duration{}
	for {
		select {
		case <-ctx.Done():
			fmt.Fprintln(w, "supervisor: shutting down children")
			mu.Lock()
			for _, ch := range children {
				if ch.cmd.Process != nil {
					_ = ch.cmd.Process.Signal(os.Interrupt)
				}
			}
			mu.Unlock()
			time.Sleep(2 * time.Second)
			mu.Lock()
			for _, ch := range children {
				if ch.cmd.ProcessState == nil || !ch.cmd.ProcessState.Exited() {
					_ = ch.cmd.Process.Kill()
				}
			}
			mu.Unlock()
			return ctx.Err()
		case pool := <-errCh:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			d := backoff[pool]
			if d == 0 {
				d = time.Second
			} else {
				d *= 2
				if d > 30*time.Second {
					d = 30 * time.Second
				}
			}
			backoff[pool] = d
			fmt.Fprintf(w, "supervisor: restarting pool %s after %s\n", pool, d)
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return ctx.Err()
			}
			if err := startChild(pool); err != nil {
				fmt.Fprintf(w, "supervisor: restart failed for %s: %v\n", pool, err)
				go func() { errCh <- pool }()
				continue
			}
			backoff[pool] = 0
			mu.Lock()
			newCmd := children[pool].cmd
			mu.Unlock()
			go func(p string, c *exec.Cmd) {
				err := c.Wait()
				if ctx.Err() != nil {
					return
				}
				if err != nil {
					fmt.Fprintf(w, "supervisor: pool %s pid=%d exited: %v — restarting\n", p, c.Process.Pid, err)
				}
				errCh <- p
			}(pool, newCmd)
		}
	}
}

func (wo *Worker) runPool(ctx context.Context, w io.Writer, getenv func(string) string, poolType string) error {
	cfg, ok := poolConfigs[poolType]
	if !ok {
		return fmt.Errorf("unknown pool type %q", poolType)
	}
	redisAddr := getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = getenv("RedisURI")
	}
	if redisAddr == "" {
		redisAddr = "redis://123456890ca@localhost:6379/0"
	}
	if !strings.Contains(redisAddr, "://") {
		redisAddr = "redis://" + redisAddr
	}

	fmt.Fprintf(w, "pool %s: connecting to redis %s pid=%d\n", poolType, redisAddr, os.Getpid())

	store, err := broker.Redis_Connect(redisAddr)
	if err != nil {
		return fmt.Errorf("pool %s: redis connect failed: %w", poolType, err)
	}
	defer store.Conn.Close()

	poolSize := dynamicPoolSize(getenv)
	fmt.Fprintf(w, "pool %s: starting dispatcher with %d workers (GOMAXPROCS=%d)\n", poolType, poolSize, runtime.GOMAXPROCS(0))

	disp := &dispatcher.Dispatcher{}
	d, err := disp.NewDispatcher(poolSize)
	if err != nil {
		return fmt.Errorf("pool %s: dispatcher create failed: %w", poolType, err)
	}
	// Per-pool Register handler exposed to worker (Q3) — each pool registers its job type
	switch poolType {
	case "email":
		d.Register("email", func(ctx context.Context, job *task.Job) error {
			b, _ := json.Marshal(job.PAYLOAD)
			_, err := wo.ProcessJobs(job.TYPE, string(b), ctx)
			return err
		})
	case "file_processing":
		d.Register("file", func(ctx context.Context, job *task.Job) error {
			b, _ := json.Marshal(job.PAYLOAD)
			_, err := wo.ProcessJobs(job.TYPE, string(b), ctx)
			return err
		})
	case "exec_processing":
		d.Register("exec", func(ctx context.Context, job *task.Job) error {
			b, _ := json.Marshal(job.PAYLOAD)
			_, err := wo.ProcessJobs(job.TYPE, string(b), ctx)
			return err
		})
	}
	go d.Run(ctx)

	workerPoolCh := make(chan *Worker, poolSize)

	var wg sync.WaitGroup
	for i := 0; i < poolSize; i++ {
		wkr, err := (&Worker{}).NewWorker(i, workerPoolCh, store.Conn)
		if err != nil {
			return fmt.Errorf("pool %s: worker %d create failed: %w", poolType, i, err)
		}
		wkr.PoolID = poolType
		workerPoolCh <- wkr
		// Register idle worker's JobChannel with dispatcher (real dispatcher uses chan chan *task.Job)
		d.WorkerPool <- wkr.JobChannel
		wg.Add(1)
		go func(wk *Worker) {
			defer wg.Done()
			workerLoop(ctx, wk, store, w, workerPoolCh, d.WorkerPool)
		}(wkr)
	}

	go heartbeatLoop(ctx, store.Conn, poolType, w)

	consumerBase := fmt.Sprintf("%s-pool-%d", poolType, os.Getpid())
	highConsumer := consumerBase + "-high"
	lowConsumer := consumerBase + "-low"

	serveErrCh := make(chan error, 2)
	go func() {
		fmt.Fprintf(w, "pool %s: serving high stream %s group %s consumer %s\n", poolType, cfg.HighStream, cfg.HighGroup, highConsumer)
		serveErrCh <- store.ServeJobs(ctx, cfg.HighStream, cfg.HighGroup, highConsumer, d)
	}()
	go func() {
		fmt.Fprintf(w, "pool %s: serving low stream %s group %s consumer %s\n", poolType, cfg.LowStream, cfg.LowGroup, lowConsumer)
		serveErrCh <- store.ServeJobs(ctx, cfg.LowStream, cfg.LowGroup, lowConsumer, d)
	}()

	select {
	case <-ctx.Done():
		fmt.Fprintf(w, "pool %s: context done, draining\n", poolType)
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			fmt.Fprintf(w, "pool %s: drain timeout\n", poolType)
		}
		return ctx.Err()
	case err := <-serveErrCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			fmt.Fprintf(w, "pool %s: ServeJobs exited: %v\n", poolType, err)
			return err
		}
		return err
	}
}

func workerLoop(ctx context.Context, wkr *Worker, store *broker.RedisStore, logW io.Writer, workerPoolCh chan *Worker, dispPool chan chan *task.Job) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-wkr.JobChannel:
			func() {
				defer func() {
					if r := recover(); r != nil {
						fmt.Fprintf(logW, "pool %s worker %d panic on job %s: %v\n", wkr.PoolID, wkr.ID, job.ID, r)
						_ = store.HandleJobFailure(ctx, job.ID, streamForType(job.TYPE), groupForType(job.TYPE), job.ATTEMPT)
					}
					select {
					case workerPoolCh <- wkr:
					case <-ctx.Done():
					}
					select {
					case dispPool <- wkr.JobChannel:
					case <-ctx.Done():
					}
				}()

				var payloadStr string
				switch v := job.PAYLOAD.(type) {
				case string:
					payloadStr = v
				case []byte:
					payloadStr = string(v)
				default:
					if b, err := json.Marshal(v); err == nil {
						payloadStr = string(b)
					} else {
						payloadStr = fmt.Sprintf("%v", v)
					}
				}
				result, err := wkr.ProcessJobs(job.TYPE, payloadStr, ctx)
				if err != nil {
					fmt.Fprintf(logW, "pool %s worker %d job %s type %s failed: %v\n", wkr.PoolID, wkr.ID, job.ID, job.TYPE, err)
					_ = store.HandleJobFailure(ctx, job.ID, streamForType(job.TYPE), groupForType(job.TYPE), job.ATTEMPT)
					return
				}
				stream := streamForType(job.TYPE)
				group := groupForType(job.TYPE)
				if stream != "" && group != "" {
					if _, ackErr := store.AckJob(ctx, stream, group, job.ID); ackErr != nil {
						fmt.Fprintf(logW, "pool %s worker %d ack failed %s: %v\n", wkr.PoolID, wkr.ID, job.ID, ackErr)
					} else {
						_ = result
					}
				}
			}()
		}
	}
}

func heartbeatLoop(ctx context.Context, conn *redis.Client, poolType string, w io.Writer) {
	key := fmt.Sprintf("heartbeat:workers:%s:%d", poolType, os.Getpid())
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	_ = conn.Set(ctx, key, strconv.FormatInt(time.Now().Unix(), 10), 10*time.Second).Err()
	for {
		select {
		case <-ctx.Done():
			_ = conn.Del(ctx, key).Err()
			return
		case <-ticker.C:
			if err := conn.Set(ctx, key, strconv.FormatInt(time.Now().Unix(), 10), 10*time.Second).Err(); err != nil {
				fmt.Fprintf(w, "pool %s heartbeat failed: %v\n", poolType, err)
			}
		}
	}
}

func streamForType(jobType string) string {
	switch jobType {
	case "email":
		return queue.EmailHighStream
	case "file":
		return queue.FileHighStream
	case "exec":
		return queue.ExecHighStream
	default:
		return ""
	}
}

func groupForType(jobType string) string {
	switch jobType {
	case "email":
		return queue.EmailGroupHigh
	case "file":
		return queue.FileGroupHigh
	case "exec":
		return queue.ExecGroupHigh
	default:
		return ""
	}
}

/**
 * This function will claim the jobs and then send them to the
 * ProcessJobs function — delegates to broker.ServeJobs.
**/
func (w *Worker) ClaimJob(wId, job_type, job_payload string, ctx context.Context) error {
	if wId == "" {
		return errors.New("ClaimJob: wId cannot be empty")
	}
	if ctx == nil {
		return errors.New("ClaimJob: ctx cannot be nil")
	}
	poolType := job_type
	if _, ok := poolConfigs[poolType]; !ok {
		switch job_type {
		case "email":
			poolType = "email"
		case "file":
			poolType = "file_processing"
		case "exec":
			poolType = "exec_processing"
		default:
			return fmt.Errorf("ClaimJob: unknown job type %q", job_type)
		}
	}
	_ = poolType
	_ = job_payload
	return nil
}

/**
* This function process all jobs differentiating each type of job and doing what
* it requires to complete them
*
 */
func (w *Worker) ProcessJobs(job_type, job_payload string, ctx context.Context) (string, error) {
	if job_payload == "" {
		return "", errors.New("job payload cannot be empty")
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(job_payload), &payload); err != nil {
		return "", fmt.Errorf("invalid job payload: %w", err)
	}
	if job_type == "email" {
		if _, ok := payload["to"]; !ok {
			return "", errors.New("email job missing required field: to")
		}
		if _, ok := payload["from"]; !ok {
			return "", errors.New("email job missing required field: from")
		}
		return fmt.Sprintf("email processed from %v to %v", payload["from"], payload["to"]), nil
	} else if job_type == "exec" {
		if _, ok := payload["command"]; !ok {
			return "", errors.New("exec job missing required field: command")
		}
		return fmt.Sprintf("exec processed command %v", payload["command"]), nil
	} else if job_type == "file" {
		if _, ok := payload["filename"]; !ok {
			if _, ok2 := payload["path"]; !ok2 {
				return "", errors.New("file job missing required field: filename or path")
			}
		}
		return fmt.Sprintf("file processed %v", payload["filename"]), nil
	} else {
		return "", errors.New("Job type doesn't match with the allowed ones")
	}
}
