package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rs/zerolog"
	appsync "github.com/wealthfolio/wealthfolio-connect-self-hosted/internal/application/sync"
	domainsync "github.com/wealthfolio/wealthfolio-connect-self-hosted/internal/domain/sync"
	"github.com/wealthfolio/wealthfolio-connect-self-hosted/internal/infrastructure/clients/ibkr"
	"github.com/wealthfolio/wealthfolio-connect-self-hosted/internal/infrastructure/config"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func TestTelegram(t *testing.T) { RegisterFailHandler(Fail); RunSpecs(t, "Telegram alerts") }

type failingIBKRDoer struct{}

func (failingIBKRDoer) Do(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: 429, Body: io.NopCloser(strings.NewReader(""))}, nil
}

var _ = Describe("durable Telegram notifications", func() {
	var server *httptest.Server
	var messages chan string
	var cfg config.TelegramConfig
	var status atomic.Int32
	var calls atomic.Int32
	var notifiers []*Notifier
	critical := domainsync.Incident{Severity: "CRITICAL", Reason: "Limită IBKR Flex depășită", Code: "1018", Suspended: true}
	warning := domainsync.Incident{Severity: "WARNING", Reason: "Sincronizarea a eșuat"}
	BeforeEach(func() {
		messages = make(chan string, 100)
		status.Store(200)
		calls.Store(0)
		notifiers = nil
		cfg = config.TelegramConfig{Enabled: true, BotToken: "private-bot-token", ChatID: "1234", StateFile: filepath.Join(GinkgoT().TempDir(), "state.json")}
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			messages <- body["text"]
			w.WriteHeader(int(status.Load()))
			_, _ = io.WriteString(w, `{"ok":true}`)
		}))
	})
	AfterEach(func() {
		for _, n := range notifiers {
			Expect(n.Stop(context.Background())).To(Succeed())
		}
		server.Close()
	})
	start := func() *Notifier {
		n := New(cfg, zerolog.Nop())
		n.endpoint = server.URL
		Expect(n.Start(context.Background())).To(Succeed())
		notifiers = append(notifiers, n)
		return n
	}
	It("delivers one critical notification, deduplicates and recovers once", func() {
		n := start()
		for range 10 {
			n.Report(critical)
		}
		Eventually(messages).Should(Receive(ContainSubstring("CRITIC")))
		for range 10 {
			n.Report(critical)
		}
		Consistently(messages, 50*time.Millisecond).ShouldNot(Receive())
		n.Report(domainsync.Incident{})
		n.Report(domainsync.Incident{})
		Eventually(messages).Should(Receive(ContainSubstring("RESTABILIT")))
		Consistently(messages, 50*time.Millisecond).ShouldNot(Receive())
		n.Report(critical)
		Eventually(messages).Should(Receive(ContainSubstring("1018")))
	})
	It("preserves incident deduplication over restart and records recovery", func() {
		n := start()
		n.Report(critical)
		Eventually(messages).Should(Receive())
		Expect(n.Stop(context.Background())).To(Succeed())
		restarted := start()
		restarted.Report(critical)
		Consistently(messages, 50*time.Millisecond).ShouldNot(Receive())
		restarted.Report(domainsync.Incident{})
		Eventually(messages).Should(Receive(ContainSubstring("RESTABILIT")))
	})
	It("does not send successful periodic runs and escalates a warning once", func() {
		n := start()
		n.Report(domainsync.Incident{})
		Consistently(messages, 30*time.Millisecond).ShouldNot(Receive())
		n.Report(warning)
		Eventually(messages).Should(Receive(ContainSubstring("AVERTISMENT")))
		n.Report(domainsync.Incident{Severity: "WARNING", Reason: "Another failure"})
		n.Report(critical)
		Eventually(messages).Should(Receive(ContainSubstring("CRITIC")))
		n.Report(warning)
		n.Report(critical)
		Consistently(messages, 30*time.Millisecond).ShouldNot(Receive())
	})
	It("retains failed deliveries on disk and retries after restart", func() {
		status.Store(503)
		n := start()
		n.Report(critical)
		Eventually(messages).Should(Receive())
		Expect(n.Stop(context.Background())).To(Succeed())
		status.Store(200)
		for len(messages) > 0 {
			<-messages
		}
		restarted := start()
		Eventually(messages).Should(Receive(ContainSubstring("1018")))
		Expect(restarted.Stop(context.Background())).To(Succeed())
		data, err := os.ReadFile(cfg.StateFile)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).NotTo(ContainSubstring(cfg.BotToken))
		var state persistentState
		Expect(json.Unmarshal(data, &state)).To(Succeed())
		Expect(state.Pending).To(BeEmpty())
		Expect(state.Active).To(BeTrue())
	})
	It("drains queued critical messages on shutdown", func() {
		n := start()
		n.Report(critical)
		Expect(n.Stop(context.Background())).To(Succeed())
		Expect(messages).To(Receive(ContainSubstring("CRITIC")))
	})
	It("finishes synchronization while Telegram delivery is blocked", func() {
		server.Close()
		release := make(chan struct{})
		var once sync.Once
		defer once.Do(func() { close(release) })
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			<-release
			w.WriteHeader(503)
		}))
		n := start()
		client, err := ibkr.NewFlex("U12345678", config.IBKRFlexConfig{
			BaseURL: "https://example.com", Token: "private-ibkr-token", Cooldown: 24 * time.Hour,
		}, zerolog.Nop(), failingIBKRDoer{})
		Expect(err).NotTo(HaveOccurred())
		client.SetAlertReporter(n)
		var output bytes.Buffer
		service := appsync.NewService(appsync.Params{Logger: zerolog.New(&output), Clients: []domainsync.BrokerClient{client}})
		done := make(chan error, 1)
		go func() { done <- service.RunOnce(context.Background()) }()
		Eventually(done, time.Second).Should(Receive(BeNil()))
		once.Do(func() { close(release) })
		Expect(n.Stop(context.Background())).To(Succeed())
		Expect(output.String()).NotTo(ContainSubstring("private-ibkr-token"))
		Expect(output.String()).NotTo(ContainSubstring("U12345678"))
	})
	It("does no network or disk work when disabled", func() {
		cfg.Enabled = false
		n := start()
		n.Report(critical)
		Expect(n.Stop(context.Background())).To(Succeed())
		Expect(calls.Load()).To(BeZero())
		_, err := os.Stat(cfg.StateFile)
		Expect(os.IsNotExist(err)).To(BeTrue())
	})
	It("reports corrupt persistent state without stopping delivery", func() {
		Expect(os.WriteFile(cfg.StateFile, []byte("bad json"), 0o600)).To(Succeed())
		n := start()
		n.Report(critical)
		Eventually(messages).Should(Receive(ContainSubstring("stării persistente"))) //nolint:misspell // Romanian notification text.
		Eventually(messages).Should(Receive(ContainSubstring("CRITIC")))
	})
	It("warns once if the state directory is unwritable", func() {
		Expect(os.WriteFile(cfg.StateFile, nil, 0o600)).To(Succeed())
		cfg.StateFile = filepath.Join(cfg.StateFile, "impossible.json")
		n := start()
		n.Report(critical)
		Eventually(messages).Should(Receive(ContainSubstring("stării persistente"))) //nolint:misspell // Romanian notification text.
		Eventually(messages).Should(Receive(ContainSubstring("CRITIC")))
		Expect(n.Stop(context.Background())).To(Succeed())
		Consistently(messages, 30*time.Millisecond).ShouldNot(Receive())
	})
	It("bounds shutdown even when Telegram is stalled and preserves the outbox", func() {
		server.Close()
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
		n := New(cfg, zerolog.Nop())
		n.endpoint = server.URL
		n.client.Timeout = 30 * time.Millisecond
		Expect(n.Start(context.Background())).To(Succeed())
		notifiers = append(notifiers, n)
		n.Report(critical)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		Expect(n.Stop(ctx)).To(MatchError(context.DeadlineExceeded))
		Eventually(n.done).Should(BeClosed())
		data, err := os.ReadFile(cfg.StateFile)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(ContainSubstring("1018"))
	})
	It("persists a new critical transition when shutdown interrupts an older delivery", func() {
		server.Close()
		entered := make(chan struct{})
		server = httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			close(entered)
			<-r.Context().Done()
		}))
		data, err := json.Marshal(persistentState{Active: true, Pending: []notification{{Incident: warning, At: time.Now()}}})
		Expect(err).NotTo(HaveOccurred())
		Expect(os.WriteFile(cfg.StateFile, data, 0o600)).To(Succeed())
		n := start()
		Eventually(entered).Should(BeClosed())
		n.Report(critical)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		Expect(n.Stop(ctx)).To(MatchError(context.DeadlineExceeded))
		data, err = os.ReadFile(cfg.StateFile)
		Expect(err).NotTo(HaveOccurred())
		var state persistentState
		Expect(json.Unmarshal(data, &state)).To(Succeed())
		Expect(state.Pending).To(HaveLen(2))
		Expect(state.Critical).To(BeTrue())
	})
	It("never logs token-bearing transport errors or response bodies", func() {
		var output bytes.Buffer
		n := New(cfg, zerolog.New(&output))
		n.endpoint = "http://127.0.0.1:0"
		n.Report(critical)
		Expect(n.Start(context.Background())).To(Succeed())
		Expect(n.Stop(context.Background())).To(Succeed())
		Expect(output.String()).To(ContainSubstring("delivery failed"))
		Expect(output.String()).NotTo(ContainSubstring(cfg.BotToken))
		Expect(output.String()).NotTo(ContainSubstring(cfg.ChatID))
	})
	It("validates the Telegram API response and does not follow redirects", func() {
		server.Close()
		var response atomic.Value
		response.Store(`{"ok":false}`)
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, response.Load().(string)) }))
		n := New(cfg, zerolog.Nop())
		n.endpoint = server.URL
		Expect(n.client.Timeout).To(Equal(10 * time.Second))
		Expect(n.client.CheckRedirect(nil, nil)).To(Equal(http.ErrUseLastResponse))
		Expect(n.send(context.Background(), notification{Incident: critical})).NotTo(Succeed())
		response.Store("bad json")
		Expect(n.send(context.Background(), notification{Incident: critical})).NotTo(Succeed())
		n.endpoint = ":bad URL"
		Expect(n.send(context.Background(), notification{Incident: critical})).NotTo(Succeed())
	})
	It("formats Romanian local time with daylight saving rules", func() {
		message := formatMessage(notification{Incident: critical, At: time.Date(2026, 11, 11, 18, 15, 0, 0, time.UTC)})
		Expect(message).To(ContainSubstring("20:15 (Europe/Chisinau)"))
		Expect(message).To(ContainSubstring("24 de ore"))
		Expect(strings.Contains(message, cfg.BotToken)).To(BeFalse())
	})
	It("wires the lifecycle through fx", func() {
		app := fxtest.New(GinkgoT(), Module, fx.Supply(&config.Config{}, zerolog.Nop()), fx.Invoke(func(n *Notifier) { Expect(n).NotTo(BeNil()) }))
		app.RequireStart()
		app.RequireStop()
	})
})
