package telegram

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rs/zerolog"
	domainsync "github.com/wealthfolio/wealthfolio-connect-self-hosted/internal/domain/sync"
	"github.com/wealthfolio/wealthfolio-connect-self-hosted/internal/infrastructure/config"
)

var _ = Describe("incident identities and persistence gates", func() {
	var cfg config.TelegramConfig
	var messages chan string
	var server *httptest.Server
	warningA := domainsync.Incident{Severity: "WARNING", Reason: "Eroare HTTP", Code: "503"}
	warningB := domainsync.Incident{Severity: "WARNING", Reason: "Eroare HTTP", Code: "504"}
	BeforeEach(func() {
		cfg = config.TelegramConfig{Enabled: true, BotToken: "secret", ChatID: "123", StateFile: filepath.Join(GinkgoT().TempDir(), "store", "state.json")}
		messages = make(chan string, 100)
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			messages <- body["text"]
			_, _ = io.WriteString(w, `{"ok":true}`)
		}))
	})
	AfterEach(func() { server.Close() })
	newNotifier := func() *Notifier { n := New(cfg, zerolog.Nop()); n.endpoint = server.URL; return n }
	flush := func(n *Notifier) { n.flush(context.Background()) }
	writeState := func(state persistentState) {
		data, err := json.Marshal(state)
		Expect(err).NotTo(HaveOccurred())
		Expect(os.MkdirAll(filepath.Dir(cfg.StateFile), 0o700)).To(Succeed())
		Expect(os.WriteFile(cfg.StateFile, data, 0o600)).To(Succeed())
	}
	It("deduplicates each code and reason through restarts until one complete recovery", func() {
		n := newNotifier()
		otherReason := warningA
		otherReason.Reason = "Generarea extrasului a eșuat"
		for _, incident := range []domainsync.Incident{warningA, warningB, warningA, otherReason, warningB} {
			n.Report(incident)
		}
		flush(n)
		Expect(messages).To(Receive(ContainSubstring("503")))
		Expect(messages).To(Receive(ContainSubstring("504")))
		Expect(messages).To(Receive(ContainSubstring(otherReason.Reason)))
		Expect(messages).NotTo(Receive())
		restarted := newNotifier()
		for _, incident := range []domainsync.Incident{warningB, otherReason, warningA} {
			restarted.Report(incident)
		}
		flush(restarted)
		Expect(messages).NotTo(Receive())
		restarted.Report(domainsync.Incident{})
		restarted.Report(domainsync.Incident{})
		flush(restarted)
		Expect(messages).To(Receive(ContainSubstring("RESTABILIT")))
		Expect(messages).NotTo(Receive())
		afterRecovery := newNotifier()
		afterRecovery.Report(domainsync.Incident{})
		flush(afterRecovery)
		Expect(messages).NotTo(Receive())
		afterRecovery.Report(warningA)
		flush(afterRecovery)
		Expect(messages).To(Receive(ContainSubstring("503")))
		Expect(messages).NotTo(Receive())
	})
	It("retains distinct unsent incidents when a save fails and sends nothing until repair", func() {
		n := newNotifier()
		flush(n)
		// Turn the previously writable parent directory into a regular file.
		Expect(os.Remove(cfg.StateFile)).To(Succeed())
		Expect(os.Remove(filepath.Dir(cfg.StateFile))).To(Succeed())
		Expect(os.WriteFile(filepath.Dir(cfg.StateFile), []byte("blocked"), 0o600)).To(Succeed())
		n.Report(warningA)
		flush(n)
		n.Report(warningB)
		n.Report(warningA)
		flush(n)
		flush(n)
		Expect(messages).NotTo(Receive())
		Expect(n.state.Pending).To(HaveLen(3)) // Two distinct failures and one storage warning.
		Expect(os.Remove(filepath.Dir(cfg.StateFile))).To(Succeed())
		flush(n)
		Expect(messages).To(Receive(ContainSubstring("503")))
		Expect(messages).To(Receive(ContainSubstring("504")))
		Expect(messages).To(Receive(ContainSubstring("alertelor")))
		Expect(messages).NotTo(Receive())
		restarted := newNotifier()
		restarted.Report(warningA)
		restarted.Report(warningB)
		flush(restarted)
		Expect(messages).NotTo(Receive())
		restarted.Report(domainsync.Incident{})
		flush(restarted)
		Expect(messages).To(Receive(ContainSubstring("RESTABILIT")))
	})
	It("stops after a failed acknowledgement checkpoint and retries that checkpoint without resending", func() {
		server.Close()
		var sabotage atomic.Bool
		sabotage.Store(true)
		fileErrors := make(chan error, 1)
		backup := cfg.StateFile + ".before-ack"
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			if sabotage.CompareAndSwap(true, false) {
				err := os.Rename(cfg.StateFile, backup)
				if err == nil {
					err = os.Mkdir(cfg.StateFile, 0o700)
				}
				fileErrors <- err
			}
			messages <- body["text"]
			_, _ = io.WriteString(w, `{"ok":true}`)
		}))
		n := newNotifier()
		n.Report(warningA)
		n.Report(warningB)
		flush(n)
		Expect(fileErrors).To(Receive(BeNil()))
		Expect(messages).To(Receive(ContainSubstring("503")))
		Expect(messages).NotTo(Receive())
		flush(n)
		Expect(messages).NotTo(Receive())
		Expect(os.Remove(cfg.StateFile)).To(Succeed())
		Expect(os.Rename(backup, cfg.StateFile)).To(Succeed())
		flush(n)
		Expect(messages).To(Receive(ContainSubstring("504")))
		Expect(messages).To(Receive(ContainSubstring("alertelor")))
		Expect(messages).NotTo(Receive())
		restarted := newNotifier()
		restarted.Report(warningA)
		restarted.Report(warningB)
		flush(restarted)
		Expect(messages).NotTo(Receive())
	})
	It("reports a new storage outage after repair without repeating the synchronization incident", func() {
		n := newNotifier()
		n.Report(warningA)
		flush(n)
		Expect(messages).To(Receive(ContainSubstring("503")))
		for range 2 {
			Expect(os.Remove(cfg.StateFile)).To(Succeed())
			Expect(os.Remove(filepath.Dir(cfg.StateFile))).To(Succeed())
			Expect(os.WriteFile(filepath.Dir(cfg.StateFile), nil, 0o600)).To(Succeed())
			n.Report(warningA)
			flush(n)
			flush(n)
			Expect(messages).NotTo(Receive())
			Expect(os.Remove(filepath.Dir(cfg.StateFile))).To(Succeed())
			flush(n)
			Expect(messages).To(Receive(ContainSubstring("alertelor")))
			Expect(messages).NotTo(Receive())
		}
	})
	It("keeps queued events while a state read fails and reloads persisted identities after repair", func() {
		Expect(os.MkdirAll(cfg.StateFile, 0o700)).To(Succeed()) // Reading a directory as JSON must fail.
		n := newNotifier()
		n.Report(warningA)
		flush(n)
		Expect(messages).NotTo(Receive())
		Expect(n.loaded).To(BeFalse())
		Expect(os.Remove(cfg.StateFile)).To(Succeed())
		writeState(persistentState{Active: true, Incidents: []domainsync.Incident{warningA}})
		flush(n)
		Expect(messages).To(Receive(ContainSubstring("alertelor")))
		Expect(messages).NotTo(Receive())
		Expect(n.state.Incidents).To(Equal([]domainsync.Incident{warningA}))
	})
	It("does not replace a corrupt state if its diagnostic archive cannot be written", func() {
		// A valid long filename exceeds the filesystem component limit only when
		// the archive suffix is added. This fails the archive on Windows and Linux
		// without depending on permission checks (which root can bypass).
		cfg.StateFile = filepath.Join(filepath.Dir(cfg.StateFile), strings.Repeat("s", 240)+".json")
		Expect(os.MkdirAll(filepath.Dir(cfg.StateFile), 0o700)).To(Succeed())
		original := []byte("{ corrupt }")
		Expect(os.WriteFile(cfg.StateFile, original, 0o600)).To(Succeed())
		n := newNotifier()
		n.Report(warningA)
		flush(n)
		Expect(messages).NotTo(Receive())
		Expect(n.loaded).To(BeFalse())
		preserved, err := os.ReadFile(cfg.StateFile)
		Expect(err).NotTo(HaveOccurred())
		Expect(preserved).To(Equal(original))
	})
	DescribeTable("archives malformed and null JSON before starting clean", func(original string) {
		Expect(os.MkdirAll(filepath.Dir(cfg.StateFile), 0o700)).To(Succeed())
		Expect(os.WriteFile(cfg.StateFile, []byte(original), 0o600)).To(Succeed())
		n := newNotifier()
		n.Report(warningA)
		flush(n)
		backups, err := filepath.Glob(cfg.StateFile + ".corrupt-*")
		Expect(err).NotTo(HaveOccurred())
		Expect(backups).To(HaveLen(1))
		preserved, err := os.ReadFile(backups[0])
		Expect(err).NotTo(HaveOccurred())
		Expect(string(preserved)).To(Equal(original))
		Expect(messages).To(Receive(ContainSubstring("503")))
		Expect(messages).To(Receive(ContainSubstring("alertelor")))
		Expect(messages).NotTo(Receive())
		restarted := newNotifier()
		restarted.Report(warningA)
		flush(restarted)
		Expect(messages).NotTo(Receive())
	}, Entry("invalid JSON", "bad json"), Entry("null JSON", "null"),
		Entry("unknown fields", `{"misspelled_active":true}`),
		Entry("inconsistent flags", `{"active":false,"critical":true}`),
		Entry("unsupported severity", `{"active":true,"incidents":[{"severity":"invalid"}]}`),
		Entry("trailing data", `{"active":false} {"active":true}`))
	It("migrates identities available in an older pending outbox", func() {
		writeState(persistentState{Active: true, Pending: []notification{{Incident: warningA, At: time.Now()}}})
		n := newNotifier()
		n.Report(warningA)
		flush(n)
		Expect(messages).To(Receive(ContainSubstring("503")))
		Expect(messages).NotTo(Receive())
		restarted := newNotifier()
		restarted.Report(warningA)
		flush(restarted)
		Expect(messages).NotTo(Receive())
	})
	It("does not record arbitrary state contents in logs", func() {
		Expect(os.MkdirAll(filepath.Dir(cfg.StateFile), 0o700)).To(Succeed())
		Expect(os.WriteFile(cfg.StateFile, []byte("private-bot-token U12345678 bad json"), 0o600)).To(Succeed())
		var output strings.Builder
		n := New(cfg, zerolog.New(&output))
		n.endpoint = server.URL
		flush(n)
		Expect(output.String()).NotTo(ContainSubstring("private-bot-token"))
		Expect(output.String()).NotTo(ContainSubstring("U12345678"))
	})
})
