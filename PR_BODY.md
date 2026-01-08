Title: Add verbose CLI flag (-v) with redacted HTTP logging

Short description:
- Add a global `-v/--verbose` CLI flag to enable redacted HTTP request/response logging to help debug interactions with the Databus and Vault.

What changed:
- Add global `-v/--verbose` option to `databusclient` CLI and propagate it to API calls.
- Implement redacted HTTP logging helper (redacts `Authorization` and `X-API-KEY` headers).
- Instrument `download` and Vault token exchange flows to print HTTP request/response details when `-v` is enabled.
- Add unit tests ensuring verbose logs are printed and sensitive tokens are redacted.
- Update `README.md` and add a `CHANGELOG.md` entry.

Why:
- Provides safe, actionable debugging output for issues involving HTTP communication and auth problems without exposing secrets.

Security note:
- Authorization and API-key headers are redacted in verbose output. Avoid enabling verbose output in public CI logs.

Closes #27
