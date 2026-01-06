# Changelog

## 0.14.1 - 2026-01-01

- Add `-v/--verbose` global CLI option to enable redacted HTTP request/response logging for debugging. (CLI: `databusclient -v ...`)
- Ensure `Authorization` and `X-API-KEY` headers are redacted in verbose output.
- Add unit tests and README documentation for verbose mode.
