# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added
- Workflow download steps support an optional `user_agent` field.
- Download `--user-agent` CLI option and `user_agent` Python API argument to override the header on file HEAD and GET requests, including redirects and authentication retries. Download manifests preserve the override for replay.

### Changed
- File downloads now identify as `databus-python-client/<version> (https://github.com/dbpedia/databus-python-client)` instead of the Requests library default.

## [0.15] - 2025-12-31

### Added
- Vault authentication improvements with host-restricted token exchange
- Comprehensive tests for Vault authentication behavior
- Enhanced docstrings across all modules for better documentation coverage
- Support for download redirect handling

### Fixed
- Vault token exchange now restricted to known hosts for improved security
- Clearer authentication error messages
- README instructions now consistent with PyPI release

### Changed
- Updated CLI usage documentation to reflect current command structure
- Improved error handling in download operations

### Notes
- Version 0.15 skips 0.13 and 0.14 as requested in issue #35
- This release updates the PyPI package to align with current repository features
