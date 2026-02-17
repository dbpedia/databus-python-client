# Release Notes for databusclient 0.15

## Overview
This release addresses issue #35 by providing a new PyPI package (version 0.15) to ensure `pip install databusclient` provides the latest CLI features and bug fixes.

## Version
**0.15** (skipping 0.13 and 0.14 as requested)

## What's New

### Features & Improvements
- **Vault Authentication Enhancement**: Host-restricted token exchange for improved security
- **Better Error Messages**: Clearer authentication error messages for easier debugging
- **Download Redirect Handling**: Improved handling of redirects during file downloads
- **Comprehensive Documentation**: Enhanced docstrings across all modules

### Bug Fixes
- Fixed Vault token exchange to only work with known hosts
- Improved error handling in download operations
- Aligned README with current CLI behavior

### Testing
- Added comprehensive tests for Vault authentication
- Improved test coverage overall

## Installation

After this release is published to PyPI, users can install or upgrade with:

```bash
pip install databusclient==0.15
# or to upgrade
pip install --upgrade databusclient
```

## Build Artifacts

The following distribution files have been created and validated:
- `databusclient-0.15-py3-none-any.whl` (wheel format)
- `databusclient-0.15.tar.gz` (source distribution)

Both files have passed `twine check` validation.

## Publishing Instructions

### Prerequisites
1. PyPI account with maintainer access to the `databusclient` package
2. PyPI API token configured

### Steps to Publish

1. **Verify the build artifacts** (already done):
   ```bash
   poetry build
   twine check dist/*
   ```

2. **Upload to TestPyPI** (recommended first):
   ```bash
   twine upload --repository testpypi dist/*
   ```
   Then test installation:
   ```bash
   pip install --index-url https://test.pypi.org/simple/ databusclient==0.15
   ```

3. **Upload to PyPI**:
   ```bash
   twine upload dist/*
   ```

4. **Create a Git tag**:
   ```bash
   git tag -a v0.15 -m "Release version 0.15"
   git push origin v0.15
   ```

5. **Create a GitHub Release**:
   - Go to GitHub repository → Releases → Draft a new release
   - Choose tag `v0.15`
   - Release title: `databusclient 0.15`
   - Copy content from CHANGELOG.md
   - Attach the dist files as release assets

## Verification

After publishing, verify the release:
```bash
pip install --upgrade databusclient==0.15
databusclient --version
databusclient --help
```

## Notes
- This release resolves issue #35
- The PyPI package will now be consistent with the repository's CLI documentation
- Version numbers 0.13 and 0.14 were intentionally skipped as requested
