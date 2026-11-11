package ibkr

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Flex request guard", func() {
	var start time.Time
	var stateFile string
	var guard *flexGuard

	BeforeEach(func() {
		start = time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
		stateFile = filepath.Join(GinkgoT().TempDir(), "state.json")
		var err error
		guard, err = newFlexGuard(20*time.Second, 24*time.Hour, 48*time.Hour, stateFile)
		Expect(err).NotTo(HaveOccurred())
		guard.now = func() time.Time { return start }
	})

	It("persists sync attempts across restart and permits the next interval", func() {
		Expect(guard.beginSync()).To(Succeed())
		Expect(guard.beginSync()).To(MatchError(ContainSubstring("next sync")))
		reopened, err := newFlexGuard(20*time.Second, 24*time.Hour, 48*time.Hour, stateFile)
		Expect(err).NotTo(HaveOccurred())
		reopened.now = func() time.Time { return start.Add(47 * time.Hour) }
		Expect(reopened.beginSync()).To(MatchError(ContainSubstring("next sync")))
		reopened.now = func() time.Time { return start.Add(48 * time.Hour) }
		Expect(reopened.beginSync()).To(Succeed())
		content, err := os.ReadFile(stateFile)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(content)).To(ContainSubstring("last_sync_start"))
		Expect(string(content)).NotTo(ContainSubstring("token"))
	})

	DescribeTable("persists and expires restrictions",
		func(status int, code string) {
			if status != 0 {
				Expect(guard.tripHTTP(status)).To(Succeed())
			} else {
				Expect(guard.tripFlex(code)).To(Succeed())
			}
			reopened, err := newFlexGuard(20*time.Second, 24*time.Hour, 48*time.Hour, stateFile)
			Expect(err).NotTo(HaveOccurred())
			reopened.now = func() time.Time { return start.Add(time.Hour) }
			Expect(reopened.reserve(context.Background())).To(MatchError(ContainSubstring("suspended")))
			Expect(reopened.beginSync()).To(MatchError(ContainSubstring("suspended")))
			reopened.now = func() time.Time { return start.Add(24 * time.Hour) }
			Expect(reopened.reserve(context.Background())).To(Succeed())
			Expect(reopened.beginSync()).To(Succeed())
		},
		Entry("HTTP forbidden", 403, ""),
		Entry("HTTP rate limit", 429, ""),
		Entry("Flex IP restriction", 0, "1013"),
		Entry("Flex rate limit", 0, "1018"),
	)

	It("does not suspend unrelated failures or shorten an existing restriction", func() {
		Expect(guard.tripHTTP(500)).To(Succeed())
		Expect(guard.tripFlex("1019")).To(Succeed())
		Expect(guard.state.BlockedUntil).To(BeZero())
		Expect(guard.tripFlex("1013")).To(Succeed())
		until := guard.state.BlockedUntil
		guard.now = func() time.Time { return start.Add(-time.Hour) }
		Expect(guard.tripHTTP(429)).To(Succeed())
		Expect(guard.state.BlockedUntil).To(Equal(until))
	})

	It("persists request spacing across restart", func() {
		Expect(guard.reserve(context.Background())).To(Succeed())
		reopened, err := newFlexGuard(20*time.Second, 24*time.Hour, 48*time.Hour, stateFile)
		Expect(err).NotTo(HaveOccurred())
		reopened.now = guard.now
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		Expect(reopened.reserve(ctx)).To(MatchError(context.DeadlineExceeded))
		Expect(reopened.state.LastRequest).To(Equal(start))
		reopened.now = func() time.Time { return start.Add(20 * time.Second) }
		Expect(reopened.reserve(context.Background())).To(Succeed())
	})

	It("paces concurrent callers using one shared interval", func() {
		guard.requestInterval = 15 * time.Millisecond
		guard.stateFile = ""
		guard.now = time.Now
		var wg sync.WaitGroup
		results := make(chan error, 3)
		began := time.Now()
		for range 3 {
			wg.Go(func() { results <- guard.reserve(context.Background()) })
		}
		wg.Wait()
		close(results)
		for err := range results {
			Expect(err).NotTo(HaveOccurred())
		}
		Expect(time.Since(began)).To(BeNumerically(">=", 30*time.Millisecond))
	})

	It("does not reserve for an already canceled context", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		Expect(guard.reserve(ctx)).To(MatchError(context.Canceled))
		Expect(guard.state.LastRequest).To(BeZero())
	})

	It("rejects corrupt or unreadable state", func() {
		Expect(os.WriteFile(stateFile, []byte("not json"), 0600)).To(Succeed())
		_, err := newFlexGuard(0, 0, 0, stateFile)
		Expect(err).To(MatchError(ContainSubstring("invalid persistent state")))
		_, err = newFlexGuard(0, 0, 0, filepath.Dir(stateFile))
		Expect(err).To(MatchError(ContainSubstring("read persistent state")))
	})

	It("refuses requests on write failure and rolls back uncommitted timestamps", func() {
		Expect(os.WriteFile(stateFile, []byte("obstruction"), 0600)).To(Succeed())
		guard.stateFile = filepath.Join(stateFile, "nested", "state.json")
		Expect(guard.beginSync()).To(MatchError(ContainSubstring("create state directory")))
		Expect(guard.state.LastSyncStart).To(BeZero())
		Expect(guard.reserve(context.Background())).To(HaveOccurred())
		Expect(guard.state.LastRequest).To(BeZero())
		Expect(guard.tripHTTP(429)).To(MatchError(ContainSubstring("persist suspension")))
		Expect(guard.reserve(context.Background())).To(MatchError(ContainSubstring("suspended")))
	})

	It("fails closed when the state destination cannot be replaced", func() {
		Expect(os.Mkdir(stateFile, 0o700)).To(Succeed())
		Expect(guard.beginSync()).To(MatchError(ContainSubstring("commit state")))
		Expect(guard.state.LastSyncStart).To(BeZero())
		leftovers, err := filepath.Glob(filepath.Join(filepath.Dir(stateFile), ".ibkr-flex-state-*"))
		Expect(err).NotTo(HaveOccurred())
		Expect(leftovers).To(BeEmpty())
	})

	It("supports an explicitly in-memory guard without cooldown", func() {
		memory, err := newFlexGuard(0, 0, 0, "")
		Expect(err).NotTo(HaveOccurred())
		Expect(memory.beginSync()).To(Succeed())
		Expect(memory.reserve(context.Background())).To(Succeed())
		Expect(memory.tripFlex("1018")).To(Succeed())
		Expect(memory.state.BlockedUntil).To(BeZero())
	})

	It("rejects negative intervals", func() {
		for _, intervals := range [][3]time.Duration{{-1, 0, 0}, {0, -1, 0}, {0, 0, -1}} {
			_, err := newFlexGuard(intervals[0], intervals[1], intervals[2], "")
			Expect(err).To(MatchError(ContainSubstring("cannot be negative")))
		}
	})
})
