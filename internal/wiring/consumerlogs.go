package wiring

import (
	"piperoom/internal/clients/workerlogs"
	"piperoom/internal/tools/logtools"
)

// consumerReaders is the reader source of consumer_logs (H2): the caller's
// Wired.ConsumerLogs when set, else the worker log client's own Loki with a
// pipeline's selector (workerlogs.Client.WithSelector), so the two log tools
// share one URL, TLS, token and tenant. Nil (no Loki) leaves the tool
// answering "not configured".
func consumerReaders(w *Wired) logtools.ConsumerReaders {
	if w.ConsumerLogs != nil {
		return w.ConsumerLogs
	}
	if c, ok := w.WorkerLogs.(*workerlogs.Client); ok && c != nil {
		return logtools.ReadersFor(c)
	}
	return nil
}
