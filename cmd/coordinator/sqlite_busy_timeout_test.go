package main

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func TestSQLiteBusyTimeoutOnPooledConnections(t *testing.T) {
	t.Chdir(t.TempDir())
	const escapedFile = "uri ? # %.db"
	uri := (&url.URL{Scheme: "file", Opaque: (&url.URL{Path: escapedFile}).EscapedPath(),
		RawQuery: "mode=rwc&_foreign_keys=1"}).String()
	for _, tc := range []struct {
		name, dsn, filename  string
		timeout, foreignKeys int
	}{
		{"plain path", "plain # %25 +.db", "plain # %25 +.db", 100, 0},
		{"existing URI options", uri, escapedFile, 100, 1},
		{"literal leading question mark", "?literal # %.db", "?literal # %.db", 100, 0},
		{"explicit timeout", "override.db?_busy_timeout=225&_timeout=250", "override.db", 250, 0},
		{"explicit pragma", "pragma.db?_pragma=busy_timeout(250)", "pragma.db", 250, 0},
		{"memory", ":memory:", "", 100, 0},
		{"temporary", "", "", 100, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := NewSQLiteJobStore(tc.dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			for i := 0; i < 2; i++ {
				conn, err := store.db.Conn(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				var busyTimeout, foreignKeys int
				var journal, dbName, filename string
				var seq int
				if err := conn.QueryRowContext(t.Context(), "PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
					t.Fatal(err)
				}
				if err := conn.QueryRowContext(t.Context(), "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
					t.Fatal(err)
				}
				if err := conn.QueryRowContext(t.Context(), "PRAGMA journal_mode").Scan(&journal); err != nil {
					t.Fatal(err)
				}
				if err := conn.QueryRowContext(t.Context(), "PRAGMA database_list").Scan(&seq, &dbName, &filename); err != nil {
					t.Fatal(err)
				}
				if busyTimeout != tc.timeout || foreignKeys != tc.foreignKeys {
					t.Fatalf("connection %d: busy_timeout=%d foreign_keys=%d", i+1, busyTimeout, foreignKeys)
				}
				if tc.filename == "" {
					if filename != "" {
						t.Fatalf("temporary or memory database unexpectedly has a filename: %q", filename)
					}
					continue
				}
				want, wantErr := os.Stat(tc.filename)
				got, gotErr := os.Stat(filename)
				if journal != "delete" || wantErr != nil || gotErr != nil || !os.SameFile(got, want) {
					t.Fatalf("connection %d: journal=%s file=%q want=%q stat errors=%v, %v",
						i+1, journal, filename, tc.filename, gotErr, wantErr)
				}
			}
			if stats := store.db.Stats(); stats.InUse != 2 || stats.MaxOpenConnections != 0 {
				t.Fatalf("expected two simultaneously reserved pool connections and the default open limit: %+v", stats)
			}
		})
	}
}

func holdSQLiteTestLock(t *testing.T, db *sql.DB, begin string) func() {
	t.Helper()
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
	})
	if _, err := conn.ExecContext(t.Context(), begin); err != nil {
		t.Fatal(err)
	}
	return func() {
		t.Helper()
		if _, err := conn.ExecContext(t.Context(), "ROLLBACK"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSQLiteBusyTimeoutGetJob(t *testing.T) {
	for _, tc := range []struct {
		name string
		code codes.Code
	}{
		{"short lock", codes.OK},
		{"long lock", codes.Internal},
		{"canceled during shutdown", codes.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newCreationTestStore(t, "sqlite").(*SQLiteJobStore)
			record := creationTestRecord()
			saveRecoveryRecord(t, store, record)
			unlock := holdSQLiteTestLock(t, store.db, "BEGIN EXCLUSIVE")
			started, loaded := make(chan struct{}), make(chan struct{})
			var rawErr error
			var elapsed time.Duration
			backing := &recoveryPassStore{JobStore: store, load: func(id int64) (JobRecord, bool, error) {
				start := time.Now()
				close(started)
				got, found, err := store.Load(id)
				rawErr, elapsed = err, time.Since(start)
				close(loaded)
				return got, found, err
			}}
			s := &CoordinatorServer{jobStore: backing, lifecycle: newCoordinatorLifecycle()}
			rpc, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctx, leave, ok := s.lifecycle.tryEnter(rpc)
			if !ok {
				t.Fatal("GetJob was not admitted")
			}
			type result struct {
				reply *runtimepb.GetJobResponse
				err   error
			}
			returned := make(chan result, 1)
			go func() {
				defer leave()
				reply, err := s.GetJob(ctx, &runtimepb.GetJobRequest{JobId: record.JobID})
				returned <- result{reply, err}
			}()
			awaitShutdownSignal(t, started)

			var shutdown chan error
			switch tc.code {
			case codes.OK:
				select {
				case got := <-returned:
					t.Fatalf("GetJob completed while the lock was held: %v", got.err)
				case <-time.After(15 * time.Millisecond):
				}
				unlock()
			case codes.Canceled:
				cancel()
				p := newCoordinatorProcess(s, 2*time.Second)
				p.closeStore = func() error {
					select {
					case <-loaded:
						return store.Close()
					default:
						return errors.New("shutdown closed the store while GetJob was still using SQLite")
					}
				}
				shutdown = make(chan error, 1)
				go func() { shutdown <- p.shutdown() }()
			}
			select {
			case got := <-returned:
				t.Logf("Load elapsed=%s sqlite=%v GetJob=%v", elapsed, rawErr, got.err)
				if status.Code(got.err) != tc.code {
					t.Fatalf("GetJob=%v, %v, want status %v", got.reply, got.err, tc.code)
				}
				if tc.code == codes.OK {
					assertGetJobSnapshot(t, got.reply, record, runtimepb.JobState_JOB_STATE_FAILED)
					return
				}
				var sqliteErr *sqlite.Error
				if got.reply != nil || !errors.As(rawErr, &sqliteErr) || sqliteErr.Code() != sqlite3.SQLITE_BUSY ||
					elapsed < 60*time.Millisecond {
					t.Fatalf("locked read: reply=%v SQLite error=%v elapsed=%s", got.reply, rawErr, elapsed)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("GetJob did not finish within the bounded observation window")
			}
			if tc.code == codes.Canceled {
				select {
				case err := <-shutdown:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("shutdown did not finish after the GetJob joined")
				}
			}
			unlock()
		})
	}
}

func TestSQLiteBusyTimeoutTerminalSaveAndGetJob(t *testing.T) {
	store := newCreationTestStore(t, "sqlite").(*SQLiteJobStore)
	s := &CoordinatorServer{jobStore: store}
	record := JobRecord{JobID: 3020, State: JobRunning, TaskType: "hash", Payload: "input", TimeoutMs: 1000, WorkerID: "worker"}
	attempt, err := s.startAttempt(record.JobID, record.WorkerID, time.Minute, record.TaskType, record.Payload, record.TimeoutMs)
	if err != nil || attempt != 1 {
		t.Fatalf("publish attempt: id=%d error=%v", attempt, err)
	}
	record.AttemptID = attempt
	terminal := withWorkerResult(record, "verified output", "")
	terminal.State = JobSucceeded
	// RESERVED permits the running snapshot to be observed while the terminal
	// writer is waiting for the same store's other write reservation.
	unlock := holdSQLiteTestLock(t, store.db, "BEGIN IMMEDIATE")
	started := make(chan struct{})
	s.jobStore = &recoveryPassStore{JobStore: store, save: func(r JobRecord) error {
		close(started)
		return store.Save(r)
	}}
	finished := make(chan error, 1)
	start := time.Now()
	go func() {
		eligible, err := s.persistTerminalResult(t.Context(), terminal)
		if !eligible {
			err = errors.New("terminal result lost its attempt authorization")
		}
		finished <- err
	}()
	awaitShutdownSignal(t, started)
	req := &runtimepb.GetJobRequest{JobId: record.JobID}
	got, err := s.GetJob(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	assertGetJobSnapshot(t, got, record, runtimepb.JobState_JOB_STATE_RUNNING)
	select {
	case err := <-finished:
		t.Fatalf("terminal writer completed while the reservation was held: %v", err)
	case <-time.After(15 * time.Millisecond):
	}
	unlock()
	select {
	case err := <-finished:
		t.Logf("terminal Save elapsed=%s error=%v", time.Since(start), err)
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("terminal Save did not finish after lock release")
	}
	assertSQLiteJobRecord(t, store, terminal)
	got, err = s.GetJob(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	assertGetJobSnapshot(t, got, terminal, runtimepb.JobState_JOB_STATE_SUCCEEDED)
}
