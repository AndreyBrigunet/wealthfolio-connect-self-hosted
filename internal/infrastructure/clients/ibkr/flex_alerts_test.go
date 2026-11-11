package ibkr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rs/zerolog"
	domainsync "github.com/wealthfolio/wealthfolio-connect-self-hosted/internal/domain/sync"
	"github.com/wealthfolio/wealthfolio-connect-self-hosted/internal/infrastructure/config"
)

type recordingAlerts struct{ incidents []domainsync.Incident }

func (r *recordingAlerts) Report(i domainsync.Incident) { r.incidents = append(r.incidents, i) }

var _ = Describe("Flex incident classification", func() {
	var client *FlexClient
	var alerts *recordingAlerts
	BeforeEach(func() {
		client = &FlexClient{config: config.IBKRFlexConfig{Cooldown: 24 * time.Hour}}
		alerts = &recordingAlerts{}
		client.SetAlertReporter(alerts)
	})
	DescribeTable("classifies typed upstream failures from either endpoint", func(endpoint string, status int, code string) {
		calls := 0
		doer := flexGuardDoer(func(r *http.Request) (*http.Response, error) {
			calls++
			body := fmt.Sprintf(`<FlexStatementResponse><Status>Fail</Status><ErrorCode>%s</ErrorCode><ErrorMessage>private-token U12345678 credentials</ErrorMessage></FlexStatementResponse>`, code)
			responseStatus := status
			if endpoint == "GetStatement" && strings.HasSuffix(r.URL.Path, "SendRequest") {
				body = `<FlexStatementResponse><Status>Success</Status><ReferenceCode>ref</ReferenceCode></FlexStatementResponse>`
				responseStatus = 200
			}
			return &http.Response{StatusCode: responseStatus, Body: io.NopCloser(strings.NewReader(body))}, nil
		})
		cfg := config.IBKRFlexConfig{BaseURL: "https://example.com", Token: "private-token", Cooldown: 24 * time.Hour, SyncInterval: time.Hour}
		var err error
		client, err = NewFlex("U12345678", cfg, zerolog.Nop(), doer)
		Expect(err).NotTo(HaveOccurred())
		client.SetAlertReporter(alerts)
		_, err = client.FetchAccountSnapshot(context.Background())
		Expect(err).To(HaveOccurred())
		client.SyncFinished(err)
		Expect(alerts.incidents).To(HaveLen(1))
		Expect(alerts.incidents[0].Severity).To(Equal("CRITICAL"))
		Expect(alerts.incidents[0].Suspended).To(BeTrue())
		Expect(fmt.Sprint(alerts.incidents)).NotTo(ContainSubstring("private-token"))
		Expect(fmt.Sprint(alerts.incidents)).NotTo(ContainSubstring("U12345678"))
		expected := 1
		if endpoint == "GetStatement" {
			expected = 2
		}
		Expect(calls).To(Equal(expected))
	}, Entry("HTTP 403 sending", "SendRequest", 403, ""), Entry("HTTP 429 downloading", "GetStatement", 429, ""), Entry("1013 sending", "SendRequest", 200, "1013"), Entry("1018 downloading", "GetStatement", 200, "1018"))
	It("ignores normal cooldown without reporting recovery", func() {
		client.SyncFinished(fmt.Errorf("fetch: %w", &flexSkipError{message: "next sync permitted after ..."}))
		Expect(alerts.incidents).To(BeEmpty())
	})
	It("does not report an existing guard suspension as a new incident", func() {
		client.SyncFinished(&flexSkipError{suspended: true})
		Expect(alerts.incidents).To(BeEmpty())
		Expect((&flexSkipError{suspended: true}).SyncSkipped()).To(BeTrue())
	})
	DescribeTable("warns on genuine sync failures", func(err error, code string) {
		client.SyncFinished(err)
		Expect(alerts.incidents[0].Severity).To(Equal("WARNING"))
		Expect(alerts.incidents[0].Code).To(Equal(code))
	}, Entry("5xx", &flexHTTPError{503}, "503"), Entry("invalid token", &flexServiceError{Code: "1015", Message: "secret"}, "1015"), Entry("untrusted code", &flexServiceError{Code: "private-token"}, ""), Entry("timeout", context.DeadlineExceeded, ""), Entry("persistence", &flexStateError{errors.New("secret path")}, ""))
	It("reports success and supports disabling the reporter", func() {
		client.SyncFinished(nil)
		Expect(alerts.incidents).To(Equal([]domainsync.Incident{{}}))
		client.SetAlertReporter(nil)
		client.SyncFinished(errors.New("failure"))
		Expect(alerts.incidents).To(HaveLen(1))
	})
	It("sanitizes logs even with alerts disabled", func() {
		client.SetAlertReporter(nil)
		Expect(client.SafeSyncError(nil)).NotTo(HaveOccurred())
		Expect(client.SafeSyncError(&flexSkipError{})).To(MatchError(ContainSubstring("skipped")))
		err := client.SafeSyncError(&flexServiceError{Code: "1015", Message: "U12345678 private-token credentials"})
		Expect(err.Error()).To(ContainSubstring("1015"))
		Expect(err.Error()).NotTo(ContainSubstring("U12345678"))
		Expect(err.Error()).NotTo(ContainSubstring("private-token"))
	})
})
