// Command jobrunner is the demo application: a worker pool that consumes from
// the queue over its HTTP API, plus a dashboard for watching it happen.
//
// By default it starts and supervises the queue server itself, which is what
// makes the crash and corruption labs real — the dashboard can SIGKILL an
// actual process and show you what the log recovered.
//
//	go build -o artie-queue ./cmd/artie-queue
//	go run ./cmd/jobrunner
//	open http://localhost:8081
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/brad945/artie-queue/internal/demo"
)

func main() {
	addr := flag.String("addr", ":8081", "dashboard listen address")
	queueBin := flag.String("queue-bin", "./artie-queue", "path to the queue server binary to supervise")
	queueAddr := flag.String("queue-addr", "127.0.0.1:8080", "address to run the supervised queue server on")
	dataDir := flag.String("data-dir", "./data", "queue data directory")
	queueURL := flag.String("queue-url", "", "attach to an already-running queue server instead of supervising one (disables the crash and corruption labs)")
	workers := flag.Int("workers", 4, "initial worker count")
	flag.Parse()

	log.SetFlags(log.LstdFlags)
	events := demo.NewEvents()

	var (
		sup     *demo.Supervisor
		client  *demo.Client
		managed = *queueURL == ""
	)

	if managed {
		if _, err := os.Stat(*queueBin); err != nil {
			log.Fatalf("queue binary not found at %s\n"+
				"build it first:  go build -o artie-queue ./cmd/artie-queue\n"+
				"or point the demo at a running server:  -queue-url http://127.0.0.1:8080", *queueBin)
		}
		sup = demo.NewSupervisor(*queueBin, *dataDir, *queueAddr)
		if err := sup.Start(); err != nil {
			log.Fatalf("could not start the queue server: %v", err)
		}
		client = sup.Client()
		events.Add("started and supervised the queue server on " + *queueAddr)
		log.Printf("supervising queue server on %s (data dir %s)", *queueAddr, *dataDir)
	} else {
		client = demo.NewClient(*queueURL)
		if !client.Healthy() {
			log.Fatalf("no healthy queue server at %s", *queueURL)
		}
		events.Add("attached to an external queue server at " + *queueURL)
		log.Printf("attached to queue server at %s (crash and corruption labs disabled)", *queueURL)
	}

	runner := demo.NewRunner(client, events)
	dash := demo.NewDashboard(sup, client, runner, events, managed)
	if err := dash.EnsureQueues(); err != nil {
		log.Fatalf("creating demo queues: %v", err)
	}
	runner.SetWorkers(*workers)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           dash.Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		log.Printf("dashboard on http://localhost%s", *addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("dashboard: %v", err)
		}
	}()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
	<-sigc
	log.Print("shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	runner.Stop()
	if managed {
		sup.Kill()
	}
}
