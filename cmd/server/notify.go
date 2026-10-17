package main

import (
	"fmt"
	"log/slog"
	"net"
	"strings"

	"piperoom/internal/config"
	"piperoom/internal/notify"
	"piperoom/internal/room"
)

// pagingSetup reads the paging settings (config.ParseNotify, QA G2) into
// room.Deps.Paging: the owner.paged webhook when PIPEROOM_NOTIFY_WEBHOOK_URL
// is set, and PIPEROOM_PAGE_UNDRIVEN_AFTER. A bad value stops the start, and
// the error names the variable. Neither the URL (it may carry a token) nor
// the secret is logged.
func pagingSetup(cfg config.Config, log *slog.Logger) (room.Paging, error) {
	n, err := config.ParseNotify(cfg)
	if err != nil {
		return room.Paging{}, fmt.Errorf("server: %w", err)
	}
	p := room.Paging{UndrivenAfter: n.PageUndrivenAfter}
	if n.WebhookURL != "" {
		w, err := notify.NewWebhook(notify.WebhookConfig{
			URL: n.WebhookURL, Secret: n.WebhookSecret, PublicURL: notifyBaseURL(cfg), Logger: log,
		})
		if err != nil {
			return room.Paging{}, fmt.Errorf("server: PIPEROOM_NOTIFY_WEBHOOK_URL: %w", err)
		}
		p.Notifier = w
	} else if n.WebhookSecret != "" {
		log.Warn("server: PIPEROOM_NOTIFY_WEBHOOK_SECRET is set but PIPEROOM_NOTIFY_WEBHOOK_URL is not; no webhook is sent")
	}
	log.Info("server: paging", "webhook", n.WebhookURL != "", "signed", n.WebhookURL != "" && n.WebhookSecret != "",
		"page_undriven_after", n.PageUndrivenAfter.String())
	return p, nil
}

// notifyBaseURL is the base of the webhook's room_url: PIPEROOM_PUBLIC_URL,
// else http://<PIPEROOM_HTTP_ADDR> when the server listens on loopback (dev),
// else "" (no link; a wildcard or remote address is not a reachable link).
func notifyBaseURL(cfg config.Config) string {
	if u := strings.TrimSpace(cfg.PublicURL); u != "" {
		return u
	}
	host, _, err := net.SplitHostPort(cfg.HTTPAddr)
	if err != nil {
		return ""
	}
	if host == "localhost" {
		return "http://" + cfg.HTTPAddr
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return "http://" + cfg.HTTPAddr
	}
	return ""
}
