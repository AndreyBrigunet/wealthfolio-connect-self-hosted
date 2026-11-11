package ibkr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// flexGuard coordinates all Flex HTTP calls and persists the last attempt and
// any temporary block. Its state must be stored on a persistent volume to
// survive container restarts. It never stores a Flex token.
type flexGuard struct {
	mu              sync.Mutex
	requestInterval time.Duration
	cooldown        time.Duration
	syncInterval    time.Duration
	stateFile       string
	state           flexGuardState
	now             func() time.Time
}

type flexGuardState struct {
	LastRequest   time.Time `json:"last_request,omitempty"`
	LastSyncStart time.Time `json:"last_sync_start,omitempty"`
	BlockedUntil  time.Time `json:"blocked_until,omitempty"`
}

func newFlexGuard(requestInterval, cooldown, syncInterval time.Duration, stateFile string) (*flexGuard, error) {
	if requestInterval < 0 || cooldown < 0 || syncInterval < 0 {
		return nil, errors.New("ibkr flex guard: intervals cannot be negative")
	}

	guard := &flexGuard{
		requestInterval: requestInterval,
		cooldown:        cooldown,
		syncInterval:    syncInterval,
		stateFile:       stateFile,
		now:             time.Now,
	}
	if stateFile == "" {
		return guard, nil
	}

	content, err := os.ReadFile(stateFile) //nolint:gosec // The path comes from trusted startup configuration, never HTTP input.
	if errors.Is(err, os.ErrNotExist) {
		return guard, nil
	}
	if err != nil {
		return nil, fmt.Errorf("ibkr flex guard: read persistent state: %w", err)
	}
	if err := json.Unmarshal(content, &guard.state); err != nil {
		return nil, fmt.Errorf("ibkr flex guard: invalid persistent state: %w", err)
	}
	return guard, nil
}

// beginSync enforces a minimum time between *attempts*, including across restarts.
// This intentionally skips repeated manual syncs and fast restart loops.
func (g *flexGuard) beginSync() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.now().UTC()
	if now.Before(g.state.BlockedUntil) {
		return fmt.Errorf("ibkr flex guard: suspended until %s", g.state.BlockedUntil.Format(time.RFC3339))
	}
	next := g.state.LastSyncStart.Add(g.syncInterval)
	if g.syncInterval > 0 && !g.state.LastSyncStart.IsZero() && now.Before(next) {
		return fmt.Errorf("ibkr flex guard: next sync permitted after %s", next.Format(time.RFC3339))
	}

	previous := g.state.LastSyncStart
	g.state.LastSyncStart = now
	if err := g.saveLocked(); err != nil {
		g.state.LastSyncStart = previous
		return err
	}
	return nil
}

// reserve prevents requests being emitted too close together. It covers both
// SendRequest and GetStatement and is safe against concurrent callers.
func (g *flexGuard) reserve(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		g.mu.Lock()
		now := g.now().UTC()
		if now.Before(g.state.BlockedUntil) {
			until := g.state.BlockedUntil
			g.mu.Unlock()
			return fmt.Errorf("ibkr flex guard: suspended until %s", until.Format(time.RFC3339))
		}
		next := g.state.LastRequest.Add(g.requestInterval)
		if g.requestInterval > 0 && !g.state.LastRequest.IsZero() && now.Before(next) {
			delay := next.Sub(now)
			g.mu.Unlock()
			if err := waitContext(ctx, delay); err != nil {
				return err
			}
			continue
		}

		old := g.state.LastRequest
		g.state.LastRequest = now
		if err := g.saveLocked(); err != nil {
			g.state.LastRequest = old
			g.mu.Unlock()
			return err
		}
		g.mu.Unlock()
		return nil
	}
}

// trip makes HTTP 403/429, Flex 1013 (IP restriction), and Flex 1018 (too
// many requests) terminal for the current sync. No automatic retry occurs.
func (g *flexGuard) trip(reason string) error {
	if g.cooldown == 0 {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	until := g.now().UTC().Add(g.cooldown)
	if until.After(g.state.BlockedUntil) {
		g.state.BlockedUntil = until
	}
	if err := g.saveLocked(); err != nil {
		return fmt.Errorf("ibkr flex guard: persist suspension (%s): %w", reason, err)
	}
	return nil
}

func (g *flexGuard) tripHTTP(status int) error {
	if status == 403 || status == 429 {
		return g.trip(fmt.Sprintf("HTTP %d", status))
	}
	return nil
}

func (g *flexGuard) tripFlex(code string) error {
	if code == "1013" || code == "1018" {
		return g.trip("Flex " + code)
	}
	return nil
}

func (g *flexGuard) saveLocked() (resultErr error) {
	if g.stateFile == "" {
		return nil
	}
	dir := filepath.Dir(g.stateFile)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("ibkr flex guard: create state directory: %w", err)
	}

	encoded, err := json.MarshalIndent(g.state, "", "  ")
	if err != nil {
		return fmt.Errorf("ibkr flex guard: encode state: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".ibkr-flex-state-*")
	if err != nil {
		return fmt.Errorf("ibkr flex guard: create temporary state: %w", err)
	}
	defer func() {
		if err := os.Remove(tmp.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
			resultErr = errors.Join(resultErr, fmt.Errorf("ibkr flex guard: remove temporary state: %w", err))
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return fmt.Errorf("ibkr flex guard: set state permissions: %w", errors.Join(err, tmp.Close()))
	}
	if _, err := tmp.Write(encoded); err != nil {
		return fmt.Errorf("ibkr flex guard: write state: %w", errors.Join(err, tmp.Close()))
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("ibkr flex guard: sync state: %w", errors.Join(err, tmp.Close()))
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("ibkr flex guard: close state: %w", err)
	}
	if err := os.Rename(tmp.Name(), g.stateFile); err != nil {
		return fmt.Errorf("ibkr flex guard: commit state: %w", err)
	}
	return nil
}
