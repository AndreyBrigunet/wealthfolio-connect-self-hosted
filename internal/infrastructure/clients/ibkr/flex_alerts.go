package ibkr

import (
	"errors"
	"fmt"
	"strconv"

	domainsync "github.com/wealthfolio/wealthfolio-connect-self-hosted/internal/domain/sync"
)

// flexSkipError is a guard rejection, not a failed synchronization attempt.
type flexSkipError struct {
	message   string
	suspended bool
}

func (e *flexSkipError) Error() string { return e.message }

type flexHTTPError struct{ status int }

func (e *flexHTTPError) Error() string { return fmt.Sprintf("HTTP status %d", e.status) }

type flexStateError struct{ cause error }

func (e *flexStateError) Error() string { return e.cause.Error() }
func (e *flexStateError) Unwrap() error { return e.cause }

// SetAlertReporter attaches the optional incident reporter before synchronization starts.
func (c *FlexClient) SetAlertReporter(reporter domainsync.AlertReporter) { c.alerts = reporter }

// SyncFinished reports exactly one result for the complete application use case.
func (c *FlexClient) SyncFinished(err error) {
	if c.alerts == nil {
		return
	}
	var skip *flexSkipError
	if errors.As(err, &skip) {
		if skip.suspended {
			c.alerts.Report(domainsync.Incident{Severity: "CRITICAL", Reason: "Sincronizarea IBKR Flex este suspendată", Suspended: true})
		}
		return
	}
	if err == nil {
		c.alerts.Report(domainsync.Incident{})
		return
	}
	c.alerts.Report(c.failureIncident(err))
}

// SafeSyncError keeps upstream messages, URLs, credentials and repository account
// identifiers out of application logs, including when Telegram is disabled.
func (c *FlexClient) SafeSyncError(err error) error {
	if err == nil {
		return nil
	}
	var skip *flexSkipError
	if errors.As(err, &skip) {
		return errors.New("IBKR Flex synchronization skipped by request guard")
	}
	incident := c.failureIncident(err)
	return fmt.Errorf("%s (code: %s)", incident.Reason, incident.Code)
}

func (c *FlexClient) failureIncident(err error) domainsync.Incident {
	incident := domainsync.Incident{Severity: "WARNING", Reason: "Sincronizarea IBKR Flex a eșuat"}
	var status *flexHTTPError
	var service *flexServiceError
	var state *flexStateError
	switch {
	case errors.As(err, &status):
		incident.Reason = "Eroare HTTP de la IBKR"
		incident.Code = strconv.Itoa(status.status)
		if status.status == 403 || status.status == 429 {
			incident.Severity = "CRITICAL"
		}
	case errors.As(err, &service):
		incident.Reason = "Generarea sau descărcarea extrasului IBKR Flex a eșuat"
		// Only numeric upstream codes are safe to include.
		if n, parseErr := strconv.Atoi(service.Code); parseErr == nil && n >= 0 && n <= 9999 {
			incident.Code = strconv.Itoa(n)
		}
		if service.Code == "1013" || service.Code == "1018" {
			incident.Severity = "CRITICAL"
		}
	case errors.As(err, &state):
		incident.Reason = "Citirea sau salvarea stării persistente IBKR Flex a eșuat" //nolint:misspell // Romanian notification text.
	}
	incident.Suspended = incident.Severity == "CRITICAL" && c.config.Cooldown > 0
	return incident
}
