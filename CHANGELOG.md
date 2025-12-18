# Changelog

All notable changes to this project will be documented in this file.

## [0.15] - 2025-12-18
- Prepare new PyPI release 0.15 (skipping 0.13/0.14 as requested).
- Improve Vault authentication: host-restricted token exchange and clearer errors.
- Add tests for Vault auth behavior.
- Add docstrings to increase docstring coverage for CI.

Note: After merging this branch, publish a PyPI release (version 0.15) so
`pip install databusclient` reflects the updated CLI behavior and bug fixes.
# Changelog

## 0.15 - Prepared release

- Prepare PyPI release 0.15.
- Restrict Vault token exchange to known hosts and provide clearer auth errors.
- Add tests for Vault auth behavior.
- Documentation: note about Vault-hosts and `--vault-token` usage.

(See PR and issue tracker for details.)
