# Ensembl Downloader

A new downloader class for retrieving chromosomal VCF files from Ensembl's FTP site.

## Overview

The `EnsemblDownloader` class inherits from `MultiVCFDownloader` and provides a convenient way to download all chromosomal VCF files from Ensembl's current variation directory: `https://ftp.ensembl.org/pub/current_variation/vcf/homo_sapiens/`

## Features

### ✅ Implemented Features

1. **Automatic Chromosome Discovery**: Discovers available chromosomes from the Ensembl FTP site
2. **Flexible Chromosome Selection**: Download all chromosomes or specify a subset
3. **CSI Index Support**: Uses Ensembl's CSI index files (.csi) instead of TBI files
4. **Checksum Validation**: Fetches and uses checksums from the CHECKSUMS file
5. **Convenience Methods**: Easy-to-use class methods for common scenarios
6. **Inheritance from MultiVCFDownloader**: Leverages parallel downloading and parquet conversion
7. **Proper Error Handling**: Graceful fallback if FTP site is unreachable

### Available Chromosomes

The downloader automatically discovers and supports:
- Autosomes: 1-22
- Sex chromosomes: X, Y  
- Mitochondrial: MT

## Usage Examples

### Basic Usage

```python
from genobear import EnsemblDownloader

# Download specific chromosomes
downloader = EnsemblDownloader(chromosomes=['1', '21', 'X'])
results = downloader.download_all()

# Access downloaded files
vcf_path = results['chr1']['vcf']
index_path = results['chr1']['index']
parquet_path = results['chr1']['parquet']  # if convert_to_parquet=True
```

### Convenience Methods

```python
# Download all autosomal chromosomes (1-22)
autosomes_downloader = EnsemblDownloader.for_autosomes_only()

# Download sex chromosomes only (X, Y)
sex_downloader = EnsemblDownloader.for_sex_chromosomes_only()

# Download specific chromosomes
specific_downloader = EnsemblDownloader.for_chromosomes(['1', '2', 'X'])
```

### Configuration Options

```python
downloader = EnsemblDownloader(
    chromosomes=['21', '22'],           # Specific chromosomes
    use_csi_index=True,                 # Use CSI index files (default)
    use_checksums=True,                 # Validate with checksums (default)
    convert_to_parquet=True,            # Convert to parquet (default)
    merge_parquets=False,               # Merge all files (default: False)
    cache_subdir='ensembl_variation'    # Cache directory (default)
)
```

## File Structure

The downloader handles files with these naming patterns:
- **VCF files**: `homo_sapiens-chr{chromosome}.vcf.gz`
- **Index files**: `homo_sapiens-chr{chromosome}.vcf.gz.csi`
- **Checksums**: Fetched from `CHECKSUMS` file

## Improvements Made to Parent Class

During implementation, I identified and fixed an issue in `MultiVCFDownloader`:

### Fixed Pydantic 2 Compatibility
- **Issue**: Field names with leading underscores (`_registry`, `_download_results`) are not allowed in Pydantic 2
- **Fix**: Renamed to `registry` and `download_results` and updated all references
- **Added**: `ConfigDict(arbitrary_types_allowed=True)` to support the `pooch.Pooch` type

## Testing

Comprehensive tests have been added in `tests/test_ensembl_downloader.py`:

### Unit Tests
- Chromosome validation and normalization
- Convenience methods functionality
- URL construction
- Cache directory configuration
- CSI index file options
- Error handling with fallback

### Integration Tests
- Real chromosome discovery from Ensembl FTP site
- Checksum fetching from CHECKSUMS file

Run tests with:
```bash
# Run all tests
uv run python -m pytest tests/test_ensembl_downloader.py -v

# Run only integration tests (requires network)
uv run python -m pytest tests/test_ensembl_downloader.py::TestEnsemblDownloaderIntegration -v
```

## Demo

A demonstration script is available at `demo_ensembl_downloader.py`:

```bash
uv run python demo_ensembl_downloader.py
```

This script shows:
- Basic usage patterns
- Convenience methods
- Live chromosome discovery
- URL structure for different configurations
- Download simulation (without actual downloading)

## Package Integration

The `EnsemblDownloader` is fully integrated into the genobear package:

```python
# Import from main package
from genobear import EnsemblDownloader

# Or from downloaders module
from genobear.downloaders import EnsemblDownloader
```

## Data Sizes

Typical file sizes from Ensembl:
- **Chromosome 1**: ~1.0GB compressed
- **Chromosome 22**: ~338MB compressed  
- **Chromosome X**: ~582MB compressed
- **Chromosome Y**: ~35MB compressed
- **Chromosome MT**: ~2MB compressed

The downloader can handle all sizes efficiently with built-in progress bars and error recovery.
