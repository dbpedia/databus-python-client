# Changelog

All notable changes to this project will be documented in this file.

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
