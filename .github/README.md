# GenoBear GitHub Actions CI

This directory contains the GitHub Actions workflows for GenoBear's continuous integration.

## Workflows

### 🔄 `ci.yml` - Main CI Pipeline
**Triggers:** Push to `main`/`develop`, PRs, weekly schedule

**Jobs:**
- **lint**: Code quality checks (future: ruff, black)
- **test**: Matrix testing across Python 3.10-3.12 and Ubuntu/macOS
- **integration-test**: Real download tests (skipped on schedule to avoid rate limits)
- **build**: Package building with uv
- **notify**: Results summary

**Key Features:**
- ⏱️ **Timeout Protection**: Downloads have configurable timeouts (5-10 min default)
- 📊 **Progress Logging**: CI-friendly progress output every 30 seconds
- 🧬 **Pathogenicity Testing**: Validates the enhanced pathogenicity analysis
- 💾 **Smart Caching**: Dependencies and test data cached between runs
- 🔄 **Matrix Testing**: Multiple Python versions and OS combinations

### 🔍 `pr.yml` - Pull Request Checks  
**Triggers:** PR opened/updated/ready-for-review

**Fast checks for PRs:**
- Import and syntax validation
- Quick tests (no downloads)
- Pathogenicity functionality verification
- CLI interface testing
- Automated PR commenting with results

## Environment Variables

Control CI behavior with these environment variables:

```bash
# Enable CI mode (auto-detected in most CI environments)
GENOBEAR_CI_MODE=true

# Download timeout in seconds (default: 600)
GENOBEAR_DOWNLOAD_TIMEOUT=300

# Max concurrent downloads (default: 3, reduced to 2 in CI)
GENOBEAR_MAX_CONCURRENT_DOWNLOADS=2

# Progress logging interval in CI (default: 30 seconds)
GENOBEAR_CI_PROGRESS_INTERVAL=30

# Force unbuffered output
PYTHONUNBUFFERED=1
```

## Timeout Handling

The CI includes enhanced timeout handling for long-running operations:

### Download Functions
- **Automatic CI Detection**: Detects GitHub Actions, GitLab CI, Travis, etc.
- **Progress Logging**: Regular progress updates to prevent CI timeouts
- **Configurable Timeouts**: Environment-controlled timeout limits
- **Graceful Failure**: Continues with other tests if downloads timeout

### Example CI Output
```
📥 Starting download: clinvar.vcf.gz (45,123,456 bytes)
⏳ Download progress: clinvar.vcf.gz - 15.2% (6,890,234/45,123,456 bytes)
⏳ Download progress: clinvar.vcf.gz - 32.7% (14,756,891/45,123,456 bytes)
✅ Download completed: clinvar.vcf.gz (45,123,456 bytes)
```

## Pathogenicity Integration

The CI specifically tests the enhanced pathogenicity analysis:

- ✅ Validates that pathogenic/benign variants are detected
- ✅ Tests clinical significance extraction from INFO fields  
- ✅ Verifies pathogenicity statistics reporting
- ✅ Ensures annotation preserves pathogenicity information

## Running Locally

Test CI behavior locally:

```bash
# Set CI environment
export GENOBEAR_CI_MODE=true
export GENOBEAR_DOWNLOAD_TIMEOUT=180

# Run tests as CI would
uv run python -m pytest tests/ -v -k "not download"

# Test pathogenicity analysis
uv run python -c "
from genobear.annotation.annotate import extract_pathogenicity_stats
import polars as pl
# ... test code
"
```

## Troubleshooting

### Download Timeouts
- Check `GENOBEAR_DOWNLOAD_TIMEOUT` setting
- Verify network connectivity in CI environment
- Review download progress logs

### Test Failures
- Check test logs in failed job artifacts
- Verify pathogenicity analysis assertions
- Ensure test data availability

### CI Environment Issues
- Confirm environment variables are set
- Check Python version compatibility
- Verify uv installation and sync

## Future Enhancements

- [ ] Add code formatting checks (ruff, black)
- [ ] Implement security scanning
- [ ] Add performance benchmarking
- [ ] Release automation
- [ ] Documentation generation