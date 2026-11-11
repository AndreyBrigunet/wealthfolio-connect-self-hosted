package ibkr

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rs/zerolog"

	"github.com/wealthfolio/wealthfolio-connect-self-hosted/internal/infrastructure/config"
)

type flexGuardDoer func(*http.Request) (*http.Response, error)

func (f flexGuardDoer) Do(request *http.Request) (*http.Response, error) {
	return f(request)
}

var _ = Describe("Flex guard integration", func() {
	var cfg config.IBKRFlexConfig
	var calls int
	var body string
	var status int
	var doer flexGuardDoer

	BeforeEach(func() {
		calls, status = 0, http.StatusOK
		body = ""
		cfg = config.IBKRFlexConfig{
			BaseURL: "https://example.com", Token: "private-test-token", QueryID: "123",
			RequestInterval: time.Millisecond, Cooldown: 24 * time.Hour, SyncInterval: 48 * time.Hour,
			PollInterval: time.Millisecond, PollTimeout: time.Second,
			StateFile: filepath.Join(GinkgoT().TempDir(), "state.json"),
		}
		doer = func(request *http.Request) (*http.Response, error) {
			calls++
			return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body))}, nil
		}
	})

	DescribeTable("stops without retry and restores restrictions after restart",
		func(endpoint string, httpStatus int, code string) {
			status = httpStatus
			body = fmt.Sprintf(`<FlexStatementResponse><Status>Fail</Status><ErrorCode>%s</ErrorCode><ErrorMessage>not ready private-test-token</ErrorMessage></FlexStatementResponse>`, code)
			if endpoint == "GetStatement" {
				failure := body
				doer = func(request *http.Request) (*http.Response, error) {
					calls++
					response := failure
					if strings.HasSuffix(request.URL.Path, "/SendRequest") {
						response = `<FlexStatementResponse><Status>Success</Status><ReferenceCode>report</ReferenceCode></FlexStatementResponse>`
					}
					return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(response))}, nil
				}
			}
			client, err := NewFlex("U123", cfg, zerolog.Nop(), doer)
			Expect(err).NotTo(HaveOccurred())
			_, err = client.FetchAccountSnapshot(context.Background())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).NotTo(ContainSubstring(cfg.Token))
			expectedCalls := 1
			if endpoint == "GetStatement" {
				expectedCalls = 2
			}
			Expect(calls).To(Equal(expectedCalls))
			reopened, err := NewFlex("U123", cfg, zerolog.Nop(), doer)
			Expect(err).NotTo(HaveOccurred())
			_, err = reopened.FetchAccountSnapshot(context.Background())
			Expect(err).To(MatchError(ContainSubstring("suspended")))
			// Check request protection independently of the minimum sync interval.
			_, err = reopened.api.get(context.Background(), reopened.api.endpoint("GetStatement"))
			Expect(err).To(MatchError(ContainSubstring("suspended")))
			Expect(calls).To(Equal(expectedCalls))
			content, err := os.ReadFile(cfg.StateFile)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(content)).NotTo(ContainSubstring(cfg.Token))
		},
		Entry("HTTP 403", "SendRequest", 403, ""),
		Entry("HTTP 429", "SendRequest", 429, ""),
		Entry("SendRequest IP restriction", "SendRequest", 200, "1013"),
		Entry("SendRequest rate limit", "SendRequest", 200, "1018"),
		Entry("GetStatement IP restriction", "GetStatement", 200, "1013"),
		Entry("GetStatement rate limit", "GetStatement", 200, "1018"),
	)

	It("uses the same pacing guard for both HTTP endpoints", func() {
		cfg.RequestInterval = time.Hour
		client, err := NewFlex("U123", cfg, zerolog.Nop(), doer)
		Expect(err).NotTo(HaveOccurred())
		_, err = client.api.get(context.Background(), client.api.endpoint("SendRequest"))
		Expect(err).NotTo(HaveOccurred())
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		_, err = client.api.get(ctx, client.api.endpoint("GetStatement"))
		Expect(err).To(MatchError(context.DeadlineExceeded))
		Expect(calls).To(Equal(1))
	})

	It("persists failed attempts so manual sync and restart cannot bypass cadence", func() {
		body = `<FlexStatementResponse><Status>Fail</Status><ErrorCode>1003</ErrorCode></FlexStatementResponse>`
		client, err := NewFlex("U123", cfg, zerolog.Nop(), doer)
		Expect(err).NotTo(HaveOccurred())
		client.api.sendEvery = 0
		_, err = client.FetchAccountSnapshot(context.Background())
		Expect(err).To(HaveOccurred())
		previousCalls := calls
		_, err = client.FetchAccountSnapshot(context.Background())
		Expect(err).To(MatchError(ContainSubstring("next sync")))
		reopened, err := NewFlex("U123", cfg, zerolog.Nop(), doer)
		Expect(err).NotTo(HaveOccurred())
		_, err = reopened.FetchAccountSnapshot(context.Background())
		Expect(err).To(MatchError(ContainSubstring("next sync")))
		Expect(calls).To(Equal(previousCalls))
	})

	It("rejects invalid persistent state before making any request", func() {
		Expect(os.WriteFile(cfg.StateFile, []byte("invalid"), 0600)).To(Succeed())
		_, err := NewFlex("U123", cfg, zerolog.Nop(), doer)
		Expect(err).To(MatchError(ContainSubstring("request guard")))
		Expect(calls).To(BeZero())
	})
})
