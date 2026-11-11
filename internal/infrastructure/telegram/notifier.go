// Package telegram delivers durable, deduplicated administrator alerts.
package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
	_ "time/tzdata" // Keep Romanian local time correct in minimal containers.

	"github.com/rs/zerolog"
	domainsync "github.com/wealthfolio/wealthfolio-connect-self-hosted/internal/domain/sync"
	"github.com/wealthfolio/wealthfolio-connect-self-hosted/internal/infrastructure/config"
	"go.uber.org/fx"
)

type notification struct {
	Incident domainsync.Incident `json:"incident"`
	At       time.Time           `json:"at"`
}

type persistentState struct {
	Active    bool                  `json:"active"`
	Critical  bool                  `json:"critical"`
	Incidents []domainsync.Incident `json:"incidents,omitempty"`
	Pending   []notification        `json:"pending,omitempty"`
}

// Notifier owns one IBKR incident stream and its durable delivery outbox.
// Report only queues memory operations; disk and HTTP work run on its worker.
type Notifier struct {
	cfg                   config.TelegramConfig
	log                   zerolog.Logger
	client                *http.Client
	endpoint              string
	mu                    sync.Mutex
	events                []domainsync.Incident
	closed                bool
	wake                  chan struct{}
	stop                  chan struct{}
	done                  chan struct{}
	cancel                context.CancelFunc
	state                 persistentState
	stateFailed           bool
	loaded                bool
	storageWarningPending bool
}

// New builds a notifier without starting goroutines or making requests.
func New(cfg config.TelegramConfig, log zerolog.Logger) *Notifier {
	return &Notifier{cfg: cfg, log: log,
		client:   &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }},
		endpoint: "https://api.telegram.org", wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
}

// Start starts delivery independently of the synchronization context.
func (n *Notifier) Start(startCtx context.Context) error {
	if !n.cfg.Enabled {
		return nil
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(startCtx))
	n.cancel = cancel
	go n.run(ctx)
	return nil
}

// Stop drains queued transitions and attempts pending delivery for at most 20s.
// Failed deliveries remain on disk for the next start.
func (n *Notifier) Stop(ctx context.Context) error {
	if !n.cfg.Enabled || n.cancel == nil {
		return nil
	}
	n.mu.Lock()
	if !n.closed {
		n.closed = true
		close(n.stop)
	}
	n.mu.Unlock()
	// Reserve the final second for persisting transitions after HTTP cancellation.
	bounded, cancel := context.WithTimeout(ctx, 19*time.Second)
	defer cancel()
	select {
	case <-n.done:
		n.cancel()
		return nil
	case <-bounded.Done():
		n.cancel()
		finalSave := time.NewTimer(time.Second)
		defer finalSave.Stop()
		select {
		case <-n.done:
		case <-finalSave.C:
		}
		return bounded.Err()
	}
}

// Report queues a sanitized incident or recovery without waiting on Telegram.
func (n *Notifier) Report(incident domainsync.Incident) {
	if !n.cfg.Enabled {
		return
	}
	n.mu.Lock()
	if !n.closed && (len(n.events) == 0 || n.events[len(n.events)-1] != incident) {
		n.events = append(n.events, incident)
	}
	n.mu.Unlock()
	select {
	case n.wake <- struct{}{}:
	default:
	}
}

func (n *Notifier) run(ctx context.Context) {
	defer close(n.done)
	defer func() {
		// Cancellation can interrupt delivery of an older outbox while new critical
		// transitions are still queued. Persist those before the worker exits.
		if err := n.checkpoint(); err != nil {
			n.persistenceFailure()
		}
	}()
	n.flush(ctx)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-n.stop:
			n.flush(ctx)
			return
		case <-ctx.Done():
			return
		case <-n.wake:
			n.flush(ctx)
		case <-ticker.C:
			n.flush(ctx)
		}
	}
}

func (n *Notifier) drain() {
	n.mu.Lock()
	events := n.events
	n.events = nil
	n.mu.Unlock()
	for _, incident := range events {
		if incident.Severity == "" {
			if !n.state.Active {
				continue
			}
			n.state.Active, n.state.Critical = false, false
			n.state.Incidents = nil
		} else {
			if slices.Contains(n.state.Incidents, incident) {
				continue
			}
			n.state.Active = true
			n.state.Critical = n.state.Critical || incident.Severity == "CRITICAL"
			n.state.Incidents = append(n.state.Incidents, incident)
		}
		n.state.Pending = append(n.state.Pending, notification{incident, time.Now()})
	}
}

func (n *Notifier) persistenceFailure() {
	if n.stateFailed {
		return
	}
	n.stateFailed = true
	n.storageWarningPending = true
	n.log.Warn().Msg("Telegram alert state could not be read or saved; check volume permissions")
}

func (n *Notifier) flush(ctx context.Context) {
	if err := n.checkpoint(); err != nil {
		n.persistenceFailure()
		return
	}
	for len(n.state.Pending) > 0 && ctx.Err() == nil {
		if err := n.send(ctx, n.state.Pending[0]); err != nil {
			// Never log transport errors, request URLs, response bodies or credentials.
			n.log.Warn().Msg("Telegram notification delivery failed; retained for retry")
			return
		}
		n.state.Pending = n.state.Pending[1:]
		if err := n.checkpoint(); err != nil {
			n.persistenceFailure()
			return
		}
	}
}

// checkpoint is the delivery gate: never send an outbox that was not saved.
// A failed acknowledgement write leaves the sent item removed in memory, so
// retries in this process checkpoint that removal before sending anything else.
func (n *Notifier) checkpoint() error {
	if !n.loaded {
		if err := n.load(); err != nil {
			return err
		}
		n.loaded = true
	}
	n.drain()
	if n.storageWarningPending {
		n.state.Pending = append(n.state.Pending, notification{domainsync.Incident{
			Severity: "WARNING", Reason: "Citirea sau salvarea stării persistente a alertelor a eșuat", //nolint:misspell // Romanian notification text.
		}, time.Now()})
		n.storageWarningPending = false
	}
	if err := n.save(); err != nil {
		return err
	}
	n.stateFailed = false
	return nil
}

func (n *Notifier) send(ctx context.Context, item notification) error {
	content, err := json.Marshal(map[string]string{"chat_id": n.cfg.ChatID, "text": formatMessage(item)})
	if err != nil {
		return errors.New("encode Telegram request")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		n.endpoint+"/bot"+n.cfg.BotToken+"/sendMessage", bytes.NewReader(content))
	if err != nil {
		return errors.New("build Telegram request")
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := n.client.Do(req)
	if err != nil {
		return errors.New("telegram transport failed")
	}
	defer response.Body.Close()
	var result struct {
		OK bool `json:"ok"`
	}
	if response.StatusCode != http.StatusOK {
		return errors.New("telegram HTTP failure")
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&result); err != nil || !result.OK {
		return errors.New("telegram API failure")
	}
	return nil
}

func formatMessage(item notification) string {
	incident := item.Incident
	header := "⚠️ Wealthfolio — IBKR AVERTISMENT"
	if incident.Severity == "CRITICAL" {
		header = "🚨 Wealthfolio — IBKR CRITIC"
	}
	body := "Problema: " + incident.Reason
	if incident.Severity == "" {
		header = "✅ Wealthfolio — IBKR RESTABILIT"
		body = "Sincronizarea s-a finalizat cu succes.\nIncidentul anterior a fost rezolvat." //nolint:misspell // Romanian notification text.
	}
	if incident.Code != "" {
		body += "\nCod: " + incident.Code
	}
	if incident.Suspended {
		body += "\nProtecție: suspendare pentru cel puțin 24 de ore"
	}
	location, err := time.LoadLocation("Europe/Chisinau")
	if err != nil {
		location = time.UTC
	}
	return fmt.Sprintf("%s\n\n%s\nOra: %s (%s)", header, body, item.At.In(location).Format("2006-01-02 15:04"), location.String())
}

func (n *Notifier) load() error {
	data, err := os.ReadFile(n.cfg.StateFile)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	state, decodeErr := decodeState(data)
	if decodeErr != nil {
		// Preserve the original bytes before allowing any replacement. If the
		// archive cannot be saved, leave delivery blocked and retry the read later.
		if _, archiveErr := writeTemporaryState(filepath.Dir(n.cfg.StateFile), filepath.Base(n.cfg.StateFile)+".corrupt-*", data); archiveErr != nil {
			return fmt.Errorf("preserve corrupt Telegram state: %w", archiveErr)
		}
		n.persistenceFailure()
		return nil
	}
	// Older files did not retain incident identities. Recover what is available
	// in the outbox; previously delivered identities cannot be reconstructed.
	if state.Active && len(state.Incidents) == 0 {
		for _, item := range state.Pending {
			if item.Incident.Severity == "" {
				state.Incidents = nil
				continue
			}
			if !slices.Contains(state.Incidents, item.Incident) {
				state.Incidents = append(state.Incidents, item.Incident)
			}
		}
	}
	n.state = *state
	return nil
}

func decodeState(data []byte) (*persistentState, error) {
	var state *persistentState
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return nil, fmt.Errorf("decode alert state: %w", err)
	}
	if state == nil || (!state.Active && (state.Critical || len(state.Incidents) > 0)) {
		return nil, errors.New("invalid alert state")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.New("unexpected data after alert state")
	}
	for _, incident := range state.Incidents {
		if incident.Severity != "WARNING" && incident.Severity != "CRITICAL" {
			return nil, errors.New("invalid incident severity in alert state")
		}
	}
	return state, nil
}

func (n *Notifier) save() error {
	dir := filepath.Dir(n.cfg.StateFile)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	data, err := json.Marshal(n.state)
	if err != nil {
		return err
	}
	name, err := writeTemporaryState(dir, ".telegram-alert-*", data)
	if err != nil {
		return err
	}
	if err := os.Rename(name, n.cfg.StateFile); err != nil {
		return errors.Join(err, os.Remove(name))
	}
	return nil
}

func writeTemporaryState(dir, pattern string, data []byte) (name string, resultErr error) {
	file, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return "", err
	}
	defer func() {
		if resultErr == nil {
			return
		}
		if err := os.Remove(file.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
			resultErr = errors.Join(resultErr, fmt.Errorf("remove temporary alert state: %w", err))
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		return "", errors.Join(err, file.Close())
	}
	if _, err := file.Write(data); err != nil {
		return "", errors.Join(err, file.Close())
	}
	if err := file.Sync(); err != nil {
		return "", errors.Join(err, file.Close())
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	return file.Name(), nil
}

// Module wires delivery before sync starts, so shutdown stops sync before draining alerts.
var Module = fx.Module("infrastructure.telegram", fx.Provide(NewManaged))

// NewManaged registers the notifier's bounded lifecycle.
func NewManaged(lc fx.Lifecycle, cfg *config.Config, log zerolog.Logger) *Notifier {
	n := New(cfg.Telegram, log)
	lc.Append(fx.Hook{OnStart: n.Start, OnStop: n.Stop})
	return n
}
