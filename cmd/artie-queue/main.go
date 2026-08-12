// Command artie-queue runs the composable queue server.
//
//	artie-queue [-addr :8080] [-dir ./data]   start the server
//	artie-queue verify -dir ./data            replay every log without starting
//	artie-queue repair -dir ./data -queue q -truncate-at N
//
// verify and repair exist because of the corruption policy: a checksum
// mismatch mid-log refuses to start, and an operator needs a way to see
// exactly what is wrong and, if they decide the tail is expendable, to discard
// it deliberately rather than have the server guess on their behalf.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/brad945/artie-queue/internal/api"
	"github.com/brad945/artie-queue/internal/queue"
	"github.com/brad945/artie-queue/internal/wal"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "verify":
			os.Exit(runVerify(os.Args[2:]))
		case "repair":
			os.Exit(runRepair(os.Args[2:]))
		}
	}
	os.Exit(runServe(os.Args[1:]))
}

func runServe(args []string) int {
	fs := flag.NewFlagSet("artie-queue", flag.ExitOnError)
	addr := fs.String("addr", ":8080", "listen address")
	dir := fs.String("dir", "./data", "data directory")
	_ = fs.Parse(args)

	mgr, err := queue.OpenManager(*dir, log.Printf)
	if err != nil {
		// This is the corruption path. Exit non-zero with the offset in the
		// message; do not start serving a queue we could not fully recover.
		var ce *wal.CorruptionError
		if errors.As(err, &ce) {
			log.Printf("REFUSING TO START: %v", err)
			return 2
		}
		log.Printf("REFUSING TO START: %v", err)
		return 1
	}

	srv := &http.Server{
		Addr:              *addr,
		Handler:           api.New(mgr, log.Printf).Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		log.Printf("artie-queue listening on %s (data dir %s)", *addr, *dir)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-errc:
		log.Printf("server error: %v", err)
		mgr.Close()
		return 1
	case sig := <-sigc:
		log.Printf("received %s, shutting down", sig)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	if err := mgr.Close(); err != nil {
		log.Printf("error closing queues: %v", err)
		return 1
	}
	return 0
}

func runVerify(args []string) int {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	dir := fs.String("dir", "./data", "data directory")
	_ = fs.Parse(args)

	entries, err := os.ReadDir(*dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "verify: %v\n", err)
		return 1
	}
	bad := 0
	found := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		path := filepath.Join(*dir, e.Name(), "wal.log")
		if _, err := os.Stat(path); err != nil {
			continue
		}
		found++
		counts := map[wal.Type]int{}
		res, err := wal.Replay(path, func(rec wal.Record) error {
			counts[rec.Type]++
			return nil
		})
		if err != nil {
			bad++
			fmt.Printf("FAIL  %s\n      %v\n", path, err)
			continue
		}
		fmt.Printf("OK    %s\n      %d records, %d valid bytes", path, res.Records, res.ValidBytes)
		for _, t := range []wal.Type{wal.TypeMeta, wal.TypeEnqueue, wal.TypeAck, wal.TypeNack, wal.TypeLeaseExpire, wal.TypeDedup} {
			if counts[t] > 0 {
				fmt.Printf(", %s=%d", t, counts[t])
			}
		}
		fmt.Println()
		if res.Torn != nil {
			fmt.Printf("      WARNING: torn tail record at offset %d (%d bytes); startup will truncate it\n",
				res.Torn.Offset, res.Torn.Bytes)
		}
	}
	if found == 0 {
		fmt.Printf("no queue logs found under %s\n", *dir)
	}
	if bad > 0 {
		fmt.Fprintf(os.Stderr, "\n%d log(s) failed verification\n", bad)
		return 2
	}
	return 0
}

func runRepair(args []string) int {
	fs := flag.NewFlagSet("repair", flag.ExitOnError)
	dir := fs.String("dir", "./data", "data directory")
	name := fs.String("queue", "", "queue name")
	at := fs.Int64("truncate-at", -1, "byte offset to truncate the log to")
	_ = fs.Parse(args)

	if *name == "" || *at < 0 {
		fmt.Fprintln(os.Stderr, "repair: -queue and -truncate-at are required")
		fmt.Fprintln(os.Stderr, "        run `artie-queue verify` first; it prints the offending offset")
		return 1
	}
	path := filepath.Join(*dir, *name, "wal.log")
	st, err := os.Stat(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "repair: %v\n", err)
		return 1
	}
	if *at > st.Size() {
		fmt.Fprintf(os.Stderr, "repair: offset %d is past end of file (%d bytes)\n", *at, st.Size())
		return 1
	}

	// Never discard data without leaving a copy behind. The point of repair is
	// that a human chose to drop the tail, not that the tool decided it was
	// unimportant.
	backup := fmt.Sprintf("%s.bak.%d", path, time.Now().UnixNano())
	if err := copyFile(path, backup); err != nil {
		fmt.Fprintf(os.Stderr, "repair: could not write backup: %v\n", err)
		return 1
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "repair: %v\n", err)
		return 1
	}
	defer f.Close()
	if err := f.Truncate(*at); err != nil {
		fmt.Fprintf(os.Stderr, "repair: %v\n", err)
		return 1
	}
	if err := f.Sync(); err != nil {
		fmt.Fprintf(os.Stderr, "repair: %v\n", err)
		return 1
	}
	fmt.Printf("truncated %s from %d to %d bytes (%d bytes discarded)\n", path, st.Size(), *at, st.Size()-*at)
	fmt.Printf("original saved as %s\n", backup)
	fmt.Printf("run `artie-queue verify -dir %s` to confirm\n", *dir)
	return 0
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
