package ibkr

import (
	"context"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	domainsync "github.com/wealthfolio/wealthfolio-connect-self-hosted/internal/domain/sync"
	"github.com/wealthfolio/wealthfolio-connect-self-hosted/internal/infrastructure/config"
)

// TestFlexLiveSnapshotFallback verifies that the configured account can load
// its latest available business-day snapshot, including across weekends.
func TestFlexLiveSnapshotFallback(t *testing.T) {
	if os.Getenv("IBKR_FLEX_LIVE_AUDIT") != "1" {
		t.Skip("set IBKR_FLEX_LIVE_AUDIT=1 to audit the configured live Flex query")
	}
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewFlex(cfg.IBKR.AccountID, cfg.IBKR.Flex, zerolog.Nop(), nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := client.FetchAccountSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Accounts) != 1 || len(snapshot.Holdings) != 1 {
		t.Fatalf("unexpected live snapshot shape: accounts=%d holdings=%d", len(snapshot.Accounts), len(snapshot.Holdings))
	}
	t.Logf("SNAPSHOT balances=%d positions=%d option_positions=%d",
		len(snapshot.Holdings[0].Balances), len(snapshot.Holdings[0].Positions),
		len(snapshot.Holdings[0].OptionPositions))

	reportDay := client.latestReportDay()
	var resumed domainsync.ActivityPage
	err = client.StreamActivities(context.Background(), []domainsync.ActivitySyncState{{
		AccountID:  "ibkr-" + cfg.IBKR.AccountID,
		NextOffset: daysBetween(cfg.IBKR.Flex.HistoryStartDate, reportDay),
	}}, func(_ context.Context, page domainsync.ActivityPage) error {
		resumed = page
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resumed.Complete {
		t.Fatal("resumed live activity window did not complete")
	}
	t.Logf("RESUMED_ACTIVITY_WINDOW mapped=%d report_day=%s",
		len(resumed.Items), reportDay.Format("2006-01-02"))
}

// TestFlexLiveAudit is an explicit, opt-in diagnostic. It never prints
// credentials, account numbers, symbols, amounts, or raw statement rows.
func TestFlexLiveAudit(t *testing.T) {
	if os.Getenv("IBKR_FLEX_LIVE_AUDIT") != "1" {
		t.Skip("set IBKR_FLEX_LIVE_AUDIT=1 to audit the configured live Flex query")
	}
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	api, err := newFlexAPI(
		cfg.IBKR.Flex.BaseURL, cfg.IBKR.Flex.Token, cfg.IBKR.Flex.QueryID, nil,
		cfg.IBKR.Flex.RequestTimeout, cfg.IBKR.Flex.PollInterval, cfg.IBKR.Flex.PollTimeout,
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	from := beginningOfUTCDay(cfg.IBKR.Flex.HistoryStartDate)
	through := beginningOfUTCDay(time.Now().UTC()).AddDate(0, 0, -1)
	recordCounts := make(map[string]int)
	activityCounts := make(map[string]int)
	elements := make(map[string]int)
	attributedElements := make(map[string]int)
	fieldNames := make(map[string]map[string]struct{})
	totalMapped, totalSkipped, totalReview, missingFX, fingerprintIDs := 0, 0, 0, 0, 0
	var firstActivity, lastActivity *time.Time
	var latestReport flexReport
	windows := 0

	for !from.After(through) {
		to := from.AddDate(0, 0, maxFlexWindowDays-1)
		if to.After(through) {
			to = through
		}
		report, fetchErr := api.fetch(ctx, from, to)
		if fetchErr != nil {
			t.Fatalf("Flex window %s..%s: %v", from.Format("2006-01-02"), to.Format("2006-01-02"), fetchErr)
		}
		if accountErr := validateFlexAccount(report, cfg.IBKR.AccountID); accountErr != nil {
			t.Fatal(accountErr)
		}
		mapped := mapFlexReport(report, cfg.IBKR.AccountID, cfg.IBKR.Flex.BaseCurrency)
		latestReport = report
		windows++
		t.Logf("WINDOW %s..%s records=%d mapped=%d skipped=%d review=%d",
			from.Format("2006-01-02"), to.Format("2006-01-02"),
			len(report.Records), len(mapped.Activities), mapped.Skipped, mapped.Review)
		totalMapped += len(mapped.Activities)
		totalSkipped += mapped.Skipped
		totalReview += mapped.Review
		for kind, count := range report.AttributedElements {
			attributedElements[kind] += count
		}
		for kind, count := range report.Elements {
			elements[kind] += count
		}
		for _, record := range report.Records {
			recordCounts[record.Kind]++
			if fieldNames[record.Kind] == nil {
				fieldNames[record.Kind] = make(map[string]struct{})
			}
			for field := range record.Attrs {
				fieldNames[record.Kind][field] = struct{}{}
			}
		}
		for _, activity := range mapped.Activities {
			activityCounts[string(activity.Type)]++
			if activity.FxRate == nil && !isFlexNativeCurrencyTrade(activity.Type) && activity.Currency.Code != "" &&
				!strings.EqualFold(activity.Currency.Code, cfg.IBKR.Flex.BaseCurrency) {
				missingFX++
			}
			if strings.Contains(activity.SourceRecordID, ":fingerprint:") {
				fingerprintIDs++
			}
			date := activity.TradeDate
			if firstActivity == nil || date.Before(*firstActivity) {
				value := date
				firstActivity = &value
			}
			if lastActivity == nil || date.After(*lastActivity) {
				value := date
				lastActivity = &value
			}
		}
		from = to.AddDate(0, 0, 1)
	}

	t.Logf("SUMMARY windows=%d mapped=%d skipped=%d needs_review=%d missing_non_base_fx=%d fingerprint_ids=%d",
		windows, totalMapped, totalSkipped, totalReview, missingFX, fingerprintIDs)
	if firstActivity != nil && lastActivity != nil {
		t.Logf("ACTIVITY_COVERAGE %s..%s", firstActivity.Format("2006-01-02"), lastActivity.Format("2006-01-02"))
	}
	t.Logf("RECORD_KINDS %s", formatAuditCounts(recordCounts))
	t.Logf("ACTIVITY_TYPES %s", formatAuditCounts(activityCounts))
	t.Logf("XML_ELEMENTS %s", formatAuditCounts(elements))
	t.Logf("ATTRIBUTED_XML_TAGS %s", formatAuditCounts(attributedElements))
	if sectionErr := validateFlexSnapshotSections(latestReport); sectionErr != nil {
		t.Error(sectionErr)
	} else {
		snapshot, snapshotErr := mapFlexSnapshot(
			latestReport, cfg.IBKR.AccountID, cfg.IBKR.Flex.BaseCurrency, time.Now().UTC(),
		)
		if snapshotErr != nil {
			t.Error(snapshotErr)
		} else {
			t.Logf("SNAPSHOT accounts=%d balances=%d positions=%d option_positions=%d",
				len(snapshot.Accounts), len(snapshot.Holdings[0].Balances),
				len(snapshot.Holdings[0].Positions), len(snapshot.Holdings[0].OptionPositions))
		}
	}
	for _, kind := range sortedAuditKeys(fieldNames) {
		missing := missingFlexFields(kind, fieldNames[kind])
		if len(missing) > 0 {
			t.Logf("MISSING_QUERY_FIELDS %s: %s", kind, strings.Join(missing, ", "))
		}
	}
}

func missingFlexFields(kind string, fields map[string]struct{}) []string {
	requirements := map[string][][]string{
		"Trade": {
			{"assetcategory", "assetclass"}, {"currency"}, {"fxratetobase"}, {"symbol"},
			{"conid"}, {"tradedate", "datetime", "date"}, {"tradetime", "datetime"},
			{"buysell", "transactiontype"}, {"quantity"}, {"tradeprice", "price"},
			{"proceeds", "trademoney"}, {"ibcommission", "commission"}, {"ibexecid", "tradeid"},
		},
		"CashTransaction": {
			{"currency"}, {"fxratetobase"}, {"datetime", "date"}, {"amount"},
			{"type", "transactiontype"}, {"description"}, {"transactionid"},
		},
		"CorporateAction": {
			{"currency"}, {"fxratetobase"}, {"datetime", "date"}, {"description"},
			{"quantity", "amount", "proceeds"}, {"transactionid", "actionid"},
		},
		"Transfer": {
			{"currency"}, {"fxratetobase"}, {"date", "datetime"}, {"direction"},
			{"quantity", "positionamount", "amount"}, {"transactionid"},
		},
		"TradeTransfer": {
			{"currency"}, {"fxratetobase"}, {"tradedate", "date"}, {"direction"},
			{"quantity"}, {"tradeprice", "price"}, {"tradeid", "transactionid"},
		},
		"OptionEAE": {
			{"currency"}, {"fxratetobase"}, {"date", "datetime"}, {"transactiontype", "type"},
			{"quantity"}, {"tradeid", "transactionid"},
		},
		"ConversionRate": {
			{"reportdate", "date", "datetime"}, {"fromcurrency"}, {"tocurrency"}, {"rate"},
		},
		"SecurityInfo": {
			{"assetcategory", "assetclass"}, {"currency"}, {"symbol"}, {"conid"}, {"figi", "securityid"},
		},
		"TransactionTax": {
			{"currency"}, {"fxratetobase"}, {"date", "datetime"}, {"taxamount", "amount"},
			{"taxdescription", "description"}, {"transactionid"},
		},
		"OpenPosition": {
			{"accountid"}, {"currency"}, {"assetcategory", "assetclass"}, {"conid"},
			{"reportdate"}, {"position", "quantity"}, {"markprice", "closeprice"},
			{"costbasisprice", "openprice"}, {"fifopnlunrealized", "unrealizedpnl"},
		},
		"CashReportCurrency": {
			{"accountid"}, {"currency"}, {"reportdate", "todate"}, {"endingcash"},
		},
		"NetAssetValueByReportDateInBase": {
			{"accountid"}, {"reportdate"}, {"total"},
		},
		"EquitySummaryByReportDateInBase": {
			{"accountid"}, {"reportdate"}, {"total"},
		},
	}
	var missing []string
	for _, aliases := range requirements[kind] {
		found := false
		for _, alias := range aliases {
			if _, ok := fields[alias]; ok {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, strings.Join(aliases, "|"))
		}
	}
	return missing
}

func formatAuditCounts(counts map[string]int) string {
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+strconv.Itoa(counts[key]))
	}
	return strings.Join(parts, ", ")
}

func sortedAuditKeys(values map[string]map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
