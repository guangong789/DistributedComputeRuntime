package main

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"

	_ "modernc.org/sqlite"
)

type SQLiteJobStore struct {
	db *sql.DB
}

func NewSQLiteJobStore(path string) (*SQLiteJobStore, error) {
	store, err := openSQLiteJobStore(path)
	if err != nil {
		return nil, err
	}
	if err := store.initSchema(); err != nil {
		store.Close()
		return nil, err
	}
	return store, nil
}

func openSQLiteJobStore(path string) (*SQLiteJobStore, error) {
	db, err := sql.Open("sqlite", sqliteJobStoreDSN(path))
	if err != nil {
		return nil, err
	}
	return &SQLiteJobStore{db: db}, nil
}

// checkRetiredJobs reads only columns available before schema initialization.
// Databases without a task_type column cannot contain a named retired task.
func (s *SQLiteJobStore) checkRetiredJobs() error {
	var hasTaskType bool
	if err := s.db.QueryRow(`SELECT EXISTS (
		SELECT 1 FROM pragma_table_info('jobs') WHERE name = 'task_type' COLLATE NOCASE
	)`).Scan(&hasTaskType); err != nil {
		return fmt.Errorf("check for unfinished retired jobs: %w", err)
	}
	if !hasTaskType {
		return nil
	}
	var jobID int64
	err := s.db.QueryRow(`SELECT job_id FROM jobs
		WHERE task_type = ? COLLATE BINARY AND state IN (?, ?, ?)
		ORDER BY job_id LIMIT 1`, retiredTaskType, JobQueued, JobRunning, JobRecovering).Scan(&jobID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("check for unfinished retired jobs: %w", err)
	}
	return fmt.Errorf("job %d has unfinished retired task type %q; use a different database or finish pending work with a compatible previous release", jobID, retiredTaskType)
}

func sqliteJobStoreDSN(path string) string {
	// Set a 100 ms default on each physical connection. Appending preserves the
	// driver's precedence for an explicit timeout and leaves other options intact.
	const busyTimeout = "_busy_timeout=100"
	switch query := strings.IndexByte(path, '?'); {
	case query > 0:
		return path + "&" + busyTimeout
	case query == 0 || path == "":
		// The driver treats a leading '?' literally. An escaped URI also
		// preserves SQLite's empty filename for a temporary database.
		return "file:" + (&url.URL{Path: path}).EscapedPath() + "?" + busyTimeout
	default:
		return path + "?" + busyTimeout
	}
}

func (s *SQLiteJobStore) initSchema() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS jobs (
			job_id INTEGER PRIMARY KEY,
			state INTEGER NOT NULL,
			attempt_id INTEGER NOT NULL,
			worker_id TEXT NOT NULL,
			task_type TEXT NOT NULL DEFAULT '',
			payload TEXT NOT NULL DEFAULT '',
			timeout_ms INTEGER NOT NULL DEFAULT 0,
			has_worker_result INTEGER NOT NULL DEFAULT 0,
			output TEXT NOT NULL DEFAULT '',
			error TEXT NOT NULL DEFAULT ''
		);
	`)
	if err != nil {
		return err
	}

	if err := s.ensureJobColumn(
		"task_type",
		"TEXT NOT NULL DEFAULT ''",
	); err != nil {
		return err
	}

	if err := s.ensureJobColumn(
		"payload",
		"TEXT NOT NULL DEFAULT ''",
	); err != nil {
		return err
	}

	if err := s.ensureJobColumn(
		"timeout_ms",
		"INTEGER NOT NULL DEFAULT 0",
	); err != nil {
		return err
	}

	for _, column := range []struct{ name, definition string }{
		{"has_worker_result", "INTEGER NOT NULL DEFAULT 0"},
		{"output", "TEXT NOT NULL DEFAULT ''"},
		{"error", "TEXT NOT NULL DEFAULT ''"},
	} {
		if err := s.ensureJobColumn(column.name, column.definition); err != nil {
			return err
		}
	}
	return nil
}

// Create uses the primary key as the arbitration point, including across store
// instances. Storage and RowsAffected errors leave the commit outcome uncertain.
func (s *SQLiteJobStore) Create(record JobRecord) (bool, error) {
	result, err := s.db.Exec(`
		INSERT INTO jobs (
			job_id, state, attempt_id, worker_id, task_type, payload, timeout_ms,
			has_worker_result, output, error
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(job_id) DO NOTHING
	`, record.JobID, record.State, record.AttemptID, record.WorkerID,
		record.TaskType, record.Payload, record.TimeoutMs,
		record.HasWorkerResult, record.Output, record.Error)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected == 1, nil
}

// TryStartQueued uses one conditional UPDATE as the first-attempt arbitration
// point, including across store instances. It never inserts or rewrites the spec.
func (s *SQLiteJobStore) TryStartQueued(expected JobRecord, workerID string) (bool, error) {
	if err := validateQueuedPublication(expected, workerID); err != nil {
		return false, err
	}
	result, err := s.db.Exec(`
		UPDATE jobs SET state = ?, attempt_id = 1, worker_id = ?
		WHERE job_id = ?
			AND task_type = ? COLLATE BINARY
			AND payload = ? COLLATE BINARY
			AND timeout_ms = ?
			AND state = ? AND attempt_id = 0
			AND worker_id = '' COLLATE BINARY
			AND has_worker_result = 0
			AND output = '' COLLATE BINARY AND error = '' COLLATE BINARY
	`, JobRunning, workerID, expected.JobID, expected.TaskType, expected.Payload,
		expected.TimeoutMs, JobQueued)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	switch affected {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("start queued job: unexpected affected row count %d", affected)
	}
}

func (s *SQLiteJobStore) TryRecoverRunning(expected JobRecord) (bool, error) {
	if err := validateRunningRepair(expected); err != nil {
		return false, err
	}
	result, err := s.db.Exec(`
		UPDATE jobs SET state = ?
		WHERE job_id = ? AND state = ? AND attempt_id = ?
			AND worker_id = ? COLLATE BINARY
			AND task_type = ? COLLATE BINARY AND payload = ? COLLATE BINARY
			AND timeout_ms = ? AND has_worker_result = 0
			AND output = '' COLLATE BINARY AND error = '' COLLATE BINARY
	`, JobRecovering, expected.JobID, JobRunning, expected.AttemptID, expected.WorkerID,
		expected.TaskType, expected.Payload, expected.TimeoutMs)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	switch affected {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("recover running job: unexpected affected row count %d", affected)
	}
}

// Save inserts or updates mutable fields in one statement, guarded by exact
// stored-spec equality. Storage/result errors do not establish whether it committed.
func (s *SQLiteJobStore) Save(record JobRecord) error {
	result, err := s.db.Exec(`
		INSERT INTO jobs(
			job_id,
			state,
			attempt_id,
			worker_id,
			task_type,
			payload,
			timeout_ms,
			has_worker_result,
			output,
			error
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(job_id) DO UPDATE SET
			state = excluded.state,
			attempt_id = excluded.attempt_id,
			worker_id = excluded.worker_id,
			has_worker_result = excluded.has_worker_result,
			output = excluded.output,
			error = excluded.error
		WHERE jobs.task_type = excluded.task_type COLLATE BINARY
			AND jobs.payload = excluded.payload COLLATE BINARY
			AND jobs.timeout_ms = excluded.timeout_ms
	`,
		record.JobID,
		record.State,
		record.AttemptID,
		record.WorkerID,
		record.TaskType,
		record.Payload,
		record.TimeoutMs,
		record.HasWorkerResult,
		record.Output,
		record.Error,
	)

	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	switch affected {
	case 0:
		return ErrJobSpecConflict
	case 1:
		return nil
	default:
		return fmt.Errorf("save job: unexpected affected row count %d", affected)
	}
}

func (s *SQLiteJobStore) Load(
	jobID int64,
) (JobRecord, bool, error) {
	var record JobRecord
	var state int

	err := s.db.QueryRow(`
		SELECT
			job_id,
			state,
			attempt_id,
			worker_id,
			task_type,
			payload,
			timeout_ms,
			has_worker_result,
			output,
			error
		FROM jobs
		WHERE job_id = ?
	`, jobID).Scan(
		&record.JobID,
		&state,
		&record.AttemptID,
		&record.WorkerID,
		&record.TaskType,
		&record.Payload,
		&record.TimeoutMs,
		&record.HasWorkerResult,
		&record.Output,
		&record.Error,
	)

	if errors.Is(err, sql.ErrNoRows) {
		return JobRecord{}, false, nil
	}

	if err != nil {
		return JobRecord{}, false, err
	}

	record.State = JobState(state)

	return record, true, nil
}

func (s *SQLiteJobStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteJobStore) ListByState(
	state JobState,
) ([]JobRecord, error) {
	rows, err := s.db.Query(`
		SELECT
			job_id,
			state,
			attempt_id,
			worker_id,
			task_type,
			payload,
			timeout_ms,
			has_worker_result,
			output,
			error
		FROM jobs
		WHERE state = ?
	`, state)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []JobRecord

	for rows.Next() {
		var record JobRecord
		var storedState int

		if err := rows.Scan(
			&record.JobID,
			&storedState,
			&record.AttemptID,
			&record.WorkerID,
			&record.TaskType,
			&record.Payload,
			&record.TimeoutMs,
			&record.HasWorkerResult,
			&record.Output,
			&record.Error,
		); err != nil {
			return nil, err
		}

		record.State = JobState(storedState)

		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return records, nil
}

func (s *SQLiteJobStore) ensureJobColumn(
	name string,
	definition string,
) error {
	rows, err := s.db.Query(`PRAGMA table_info(jobs)`)
	if err != nil {
		return err
	}
	defer rows.Close()

	found := false

	for rows.Next() {
		var (
			cid        int
			columnName string
			columnType string
			notNull    int
			defaultVal any
			primaryKey int
		)

		if err := rows.Scan(
			&cid,
			&columnName,
			&columnType,
			&notNull,
			&defaultVal,
			&primaryKey,
		); err != nil {
			return err
		}

		if columnName == name {
			found = true
			break
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	if found {
		return nil
	}

	query := fmt.Sprintf(
		"ALTER TABLE jobs ADD COLUMN %s %s",
		name,
		definition,
	)

	_, err = s.db.Exec(query)
	return err
}
