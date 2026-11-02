package store

// instancelock.go: "only one MicroFlow server per database".
//
// Why: the startup sweep (MarkInterruptedExecutions) closes every
// queued/running row that has no checkpoint, on the assumption that the
// previous process is dead. If a SECOND server is started against the same
// database while the first is still working, that sweep would cancel the
// first server's live runs, and both servers would then fight over the
// scheduler, Run-All and recovery. This lock makes the second process stop
// before it touches anything.
//
// How: a Postgres session-level advisory lock held on one dedicated
// connection (NOT taken from the pool, so the small pool is unaffected).
// The server releases it automatically when the process exits or dies,
// because Postgres drops session locks when the connection closes.
//
// Not compatible with transaction-mode poolers (pgbouncer "transaction"
// mode, Supabase :6543, Neon "-pooler" host): they can hand the session to
// someone else and the lock is meaningless. Use the direct connection
// string, or set MICROFLOW_SINGLE_INSTANCE=0 to turn the guard off.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// instanceLockKey is an arbitrary fixed 64-bit id ("MFLOW") for the
// application-wide advisory lock.
const instanceLockKey int64 = 0x4D464C4F57

// ErrAnotherInstance means another MicroFlow server holds the lock.
var ErrAnotherInstance = errors.New("another MicroFlow server is already running against this database")

// InstanceLock is the held lock. Close releases it.
type InstanceLock struct {
	cfg  *pgx.ConnConfig
	mu   sync.Mutex
	conn *pgx.Conn
}

// AcquireInstanceLock takes the lock, retrying every 2s for up to wait
// (useful right after a restart, when the old process's connection may not
// have been dropped by the database yet). wait <= 0 tries exactly once.
func (s *Store) AcquireInstanceLock(ctx context.Context, wait time.Duration) (*InstanceLock, error) {
	cfg := s.pool.Config().ConnConfig.Copy()
	// Simple protocol: no server-side prepared statements, so this also
	// survives poolers that would otherwise reject them.
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	l := &InstanceLock{cfg: cfg}

	deadline := time.Now().Add(wait)
	logged := false
	for {
		err := l.tryAcquire(ctx)
		if err == nil {
			return l, nil
		}
		if !errors.Is(err, ErrAnotherInstance) {
			return nil, err
		}
		if !time.Now().Before(deadline) {
			return nil, err
		}
		if !logged {
			log.Printf("instance lock: another server holds this database; waiting up to %s for it to stop", wait)
			logged = true
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func (l *InstanceLock) tryAcquire(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(cctx, l.cfg)
	if err != nil {
		return fmt.Errorf("instance lock: connect: %w", err)
	}
	var got bool
	if err := conn.QueryRow(cctx, `SELECT pg_try_advisory_lock($1)`, instanceLockKey).Scan(&got); err != nil {
		_ = conn.Close(context.Background())
		return fmt.Errorf("instance lock: query: %w", err)
	}
	if !got {
		_ = conn.Close(context.Background())
		return ErrAnotherInstance
	}
	l.mu.Lock()
	l.conn = conn
	l.mu.Unlock()
	return nil
}

func (l *InstanceLock) current() *pgx.Conn {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.conn
}

func (l *InstanceLock) drop() {
	l.mu.Lock()
	c := l.conn
	l.conn = nil
	l.mu.Unlock()
	if c != nil {
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = c.Close(cctx)
		cancel()
	}
}

// Watch keeps the lock honest. Every interval it pings the lock connection
// (which also keeps idle-connection reapers from closing it). If the
// connection died, the database has released the lock, so Watch tries to
// take it back. If another server got it first, onLost is called once and
// Watch stops: the caller should shut this process down.
func (l *InstanceLock) Watch(ctx context.Context, interval time.Duration, onLost func(error)) {
	if interval < time.Second {
		interval = 15 * time.Second
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			conn := l.current()
			if conn != nil {
				pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				_, err := conn.Exec(pctx, `SELECT 1`)
				cancel()
				if err == nil {
					continue
				}
				if ctx.Err() != nil {
					return
				}
				log.Printf("instance lock: connection lost (%v); trying to re-acquire", err)
				l.drop()
			}
			// Connection gone: the lock is gone too. Re-take it, retrying
			// through transient database outages.
			for ctx.Err() == nil {
				err := l.tryAcquire(ctx)
				if err == nil {
					log.Printf("instance lock: re-acquired")
					break
				}
				if errors.Is(err, ErrAnotherInstance) {
					if onLost != nil {
						onLost(err)
					}
					return
				}
				log.Printf("instance lock: re-acquire failed: %v", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Second):
				}
			}
		}
	}()
}

// Close releases the lock (closing the connection drops it).
func (l *InstanceLock) Close() {
	if l != nil {
		l.drop()
	}
}
