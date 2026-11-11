package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// TelegramConfig configures optional administrator notifications.
type TelegramConfig struct {
	Enabled   bool
	BotToken  string
	ChatID    string
	StateFile string
}

func validateTelegramState(cfg TelegramConfig, flexState string) error {
	if !cfg.Enabled {
		return nil
	}
	if strings.EqualFold(filepath.Base(cfg.StateFile), "ibkr-flex-state.json") {
		return errors.New("config: Telegram alert state must not use the IBKR guard state file")
	}
	if flexState == "" {
		return nil
	}
	alertPath, alertErr := filepath.Abs(cfg.StateFile)
	flexPath, flexErr := filepath.Abs(flexState)
	if alertErr != nil || flexErr != nil {
		return errors.New("config: invalid alert or Flex state path")
	}
	alertInfo, alertStatErr := os.Stat(alertPath)
	flexInfo, flexStatErr := os.Stat(flexPath)
	if strings.EqualFold(alertPath, flexPath) || (alertStatErr == nil && flexStatErr == nil && os.SameFile(alertInfo, flexInfo)) {
		return errors.New("config: Telegram and IBKR Flex must use separate persistent state files")
	}
	return nil
}
