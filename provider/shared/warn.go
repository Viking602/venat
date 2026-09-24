package shared

import "log/slog"

// WarnDrop reports a request field that the provider wire cannot represent.
// Adapters drop the field and continue rather than failing the request; the
// warning keeps the omission visible to operators instead of silent.
func WarnDrop(providerName, field, reason string) {
	slog.Warn("dropping unsupported request field",
		"provider", providerName,
		"field", field,
		"reason", reason,
	)
}
