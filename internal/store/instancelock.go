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
// The lock is taken, health-checked and released on that SAME dedicated
// session (pg_try_advisory_lock / pg_advisory_unlock on one connection).
// Nothing here ever terminates another backend or releases a lock it does
// not own: if another server holds the lock, this one waits (bounded) or
// refuses to start. Rolling deploys are handled by the caller, which serves
// a health-only standby listener while waiting (see internal/standby).
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
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// instanceLockKey is an arbitrary fixed 64-bit id ("MFLOW") for the
// application-wide advisory lock.
const instanceLockKey int64 = 0x4D464C4F57

// lockKey is the key actually used; tests point it at a private key.
var lockKey = instanceLockKey

// ErrAnotherInstance means another MicroFlow server holds the lock.
var ErrAnotherInstance = errors.New("another MicroFlow server is already running against this database")

// AnotherInstanceError is ErrAnotherInstance plus a read-only description of
// the session holding the lock (pid, application_name, client address, start
// time, state). It carries no credentials. errors.Is(err, ErrAnotherInstance)
// is true.
type AnotherInstanceError struct{ Holder string }

func (e *AnotherInstanceError) Error() string {
	if e.Holder == "" {
		return ErrAnotherInstance.Error()
	}
	return ErrAnotherInstance.Error() + " (lock holder: " + e.Holder + ")"
}

func (e *AnotherInstanceError) Unwrap() error { return ErrAnotherInstance }

const (
	lockRetryEvery = 2 * time.Second
	lockLogEvery   = 10 * time.Second
)

// InstanceLock is the held lock. Close releases it.
type InstanceLock struct {
	cfg    *pgx.ConnConfig
	mu     sync.Mutex
	conn   *pgx.Conn
	closed bool
}

// AcquireInstanceLock takes the lock, retrying every 2s for up to wait
// (a rolling deploy: the old process only drops it once it has stopped).
// wait <= 0 tries exactly once. While waiting it logs, every 10s, who holds
// the lock. It never forces the lock away from its holder. If ctx is
// cancelled (shutdown requested) it returns ctx.Err().
func (s *Store) AcquireInstanceLock(ctx context.Context, wait time.Duration) (*InstanceLock, error) {
	cfg := s.pool.Config().ConnConfig.Copy()
	// Simple protocol: no server-side prepared statements, so this also
	// survives poolers that would otherwise reject them.
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	// Label the session so it can be recognised in pg_stat_activity.
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	host, _ := os.Hostname()
	cfg.RuntimeParams["application_name"] = "microflow-instance-lock@" + host
	l := &InstanceLock{cfg: cfg}

	start := time.Now()
	deadline := start.Add(wait)
	lastLog := time.Time{}
	for {
		err := l.tryAcquire(ctx)
		if err == nil {
			if waited := time.Since(start); waited >= lockRetryEvery {
				log.Printf("instance lock: acquired after waiting %s", waited.Round(time.Second))
			}
			return l, nil
		}
		var held *AnotherInstanceError
		if !errors.As(err, &held) {
			return nil, err
		}
		if !time.Now().Before(deadline) {
			if wait > 0 {
				log.Printf("instance lock: gave up after %s; still held by: %s", wait, holderOrUnknown(held.Holder))
			}
			return nil, err
		}
		if lastLog.IsZero() || time.Since(lastLog) >= lockLogEvery {
			log.Printf("instance lock: another server holds this database (holder: %s); waiting %s of %s for it to stop (not forcing it)",
				holderOrUnknown(held.Holder), time.Since(start).Round(time.Second), wait)
			lastLog = time.Now()
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(lockRetryEvery):
		}
	}
}

func holderOrUnknown(h string) string {
	if h == "" {
		return "unknown"
	}
	return h
}

func (l *InstanceLock) tryAcquire(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(cctx, l.cfg)
	if err != nil {
		return fmt.Errorf("instance lock: connect: %w", err)
	}
	var got bool
	if err := conn.QueryRow(cctx, `SELECT pg_try_advisory_lock($1)`, lockKey).Scan(&got); err != nil {
		_ = conn.Close(context.Background())
		return fmt.Errorf("instance lock: query: %w", err)
	}
	if !got {
		holder := lockHolder(cctx, conn)
		_ = conn.Close(context.Background())
		return &AnotherInstanceError{Holder: holder}
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		l.release(conn)
		return errors.New("instance lock: closed")
	}
	l.conn = conn
	l.mu.Unlock()
	return nil
}

// lockHolder describes the session holding the lock (read-only diagnostics).
func lockHolder(ctx context.Context, conn *pgx.Conn) string {
	var pid int64
	var app, addr, since, state string
	err := conn.QueryRow(ctx, `SELECT a.pid, COALESCE(a.application_name,''), COALESCE(host(a.client_addr),''),
		COALESCE(to_char(a.backend_start,'YYYY-MM-DD"T"HH24:MI:SSOF'),''), COALESCE(a.state,'')
		FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
		WHERE l.locktype = 'advisory' AND l.granted AND l.objsubid = 1
		  AND ((l.classid::bigint << 32) | l.objid::bigint) = $1
		LIMIT 1`, lockKey).Scan(&pid, &app, &addr, &since, &state)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("pid=%d app=%q client=%s session_since=%s state=%s", pid, app, addr, since, state)
}

func (l *InstanceLock) current() *pgx.Conn {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.conn
}

// stillHeld reports whether THIS session still owns the lock (not merely
// that the connection answers): a pooler that swapped the session would pass
// a plain ping but fail this check.
func stillHeld(ctx context.Context, conn *pgx.Conn) (bool, error) {
	var ok bool
	err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks
		WHERE locktype = 'advisory' AND granted AND objsubid = 1 AND pid = pg_backend_pid()
		  AND ((classid::bigint << 32) | objid::bigint) = $1)`, lockKey).Scan(&ok)
	return ok, err
}

// release unlocks on the same session that locked, then closes it. Closing
// alone would also drop the lock; the explicit unlock makes the hand-over
// immediate and visible in the log.
func (l *InstanceLock) release(c *pgx.Conn) {
	cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var ok bool
	if err := c.QueryRow(cctx, `SELECT pg_advisory_unlock($1)`, lockKey).Scan(&ok); err != nil {
		log.Printf("instance lock: unlock failed (%v); closing the session, which drops the lock", err)
	} else if !ok {
		log.Printf("instance lock: unlock reported the lock was not held by this session")
	}
	_ = c.Close(cctx)
}

// drop closes a connection that is already broken (the database has dropped
// the lock with it) without any unlock attempt.
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

// Watch keeps the lock honest. Every interval it checks that the lock
// session is alive AND still owns the lock (which also keeps idle-connection
// reapers from closing it). If the session died, the database has released
// the lock, so Watch tries to take it back. If another server got it first,
// onLost is called once and Watch stops: the caller should shut this process
// down. Watch stops when ctx is cancelled or the lock is Closed.
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
			if conn == nil {
				l.mu.Lock()
				closed := l.closed
				l.mu.Unlock()
				if closed {
					return
				}
			} else {
				pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				held, err := stillHeld(pctx, conn)
				cancel()
				if err == nil && held {
					continue
				}
				if ctx.Err() != nil {
					return
				}
				if err != nil {
					log.Printf("instance lock: connection lost (%v); trying to re-acquire", err)
				} else {
					log.Printf("instance lock: this session no longer holds the lock; trying to re-acquire")
				}
				l.drop()
			}
			// Lock gone: re-take it, retrying through transient database
			// outages. Never steals: if someone else holds it, we stop.
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
				l.mu.Lock()
				closed := l.closed
				l.mu.Unlock()
				if closed {
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

// Close releases the lock on the session that took it. Safe to call more
// than once and on a nil lock.
func (l *InstanceLock) Close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.closed = true
	c := l.conn
	l.conn = nil
	l.mu.Unlock()
	if c != nil {
		l.release(c)
		log.Printf("instance lock: released")
	}
}
