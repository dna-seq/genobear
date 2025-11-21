# GenoBear 🧬

A unified toolkit for downloading, converting, processing, and annotating genomic databases.

## Features

- **Download genomic databases**: Ensembl, ClinVar, dbSNP, gnomAD
- **Convert VCF to Parquet**: Efficient columnar storage for large genomic datasets with polars-bio
- **Split variants by type**: Organize variants by TSA (Trinucleotide Sequence Alteration) for efficient querying
- **Annotate VCF files**: Annotate your VCF data with reference genomic databases using lazy joins
- **CLI and Python API**: Use via command line or import as a Python library with pipeline-based workflows
- **Parallel processing**: Download and process multiple chromosome files concurrently
- **HuggingFace integration**: Upload processed datasets directly to HuggingFace Hub

## Installation

```bash
# Using uv (recommended)
uv add genobear

# Using pip
pip install genobear
```

## Quick Start

### CLI Usage

GenoBear provides two main CLI commands: `prepare` for data preparation and `annotate` for VCF annotation.

#### Data Preparation

```bash
# Download Ensembl variation VCF files (all chromosomes) with better parallelization
prepare ensembl --split --download-workers 8

# Download specific chromosomes using pattern matching
prepare ensembl --pattern "chr(21|22|X)"  # chromosomes 21, 22, and X
prepare ensembl --pattern "chr[1-9]"      # chromosomes 1-9

# Download ClinVar
prepare clinvar --dest-dir ./my_data

# Download dbSNP with specific build
prepare dbsnp --build GRCh38 --split

# Download gnomAD v4
prepare gnomad --version v4

# Control worker counts for optimal performance
prepare ensembl --workers 4 --download-workers 8 --parquet-workers 2

# Run with custom pipeline run folder for caching
prepare ensembl --run-folder ./pipeline_cache

# Get help
prepare --help
prepare ensembl --help
```

#### VCF Annotation

```bash
# Annotate a VCF file with Ensembl variations
annotate vcf input.vcf.gz -o annotated.parquet

# Pre-download reference data
annotate download-reference

# Annotate with custom cache directory
annotate vcf sample.vcf.gz --cache-dir /data/cache

# Force re-download of reference data
annotate vcf sample.vcf.gz --force-download

# Get help
annotate --help
annotate vcf --help
```

**Key Features:**
- Better parallelization with separate worker controls for downloads vs. processing
- Pipeline caching support with `--run-folder`
- Flexible configuration options
- Environment-based configuration for all settings

### Python API Usage

#### Data Preparation

```python
import genobear as gb
from pathlib import Path

# Download and convert ClinVar (GRCh38)
results = gb.PreparationPipelines.download_clinvar()

# Download and convert dbSNP (GRCh38)
results = gb.PreparationPipelines.download_dbsnp(build="GRCh38")

# Download and convert Ensembl variations with splitting by variant type
results = gb.PreparationPipelines.download_ensembl(with_splitting=True)

# Download gnomAD data
results = gb.PreparationPipelines.download_gnomad(version="v4")

# Split existing parquet files by variant type
parquet_files = [Path("clinvar.parquet")]
results = gb.PreparationPipelines.split_existing_parquets(
    parquet_files=parquet_files,
    explode_snv_alt=True
)

# Create custom pipelines
pipeline = gb.PreparationPipelines.clinvar(with_splitting=True)
results = gb.PreparationPipelines.execute(
    pipeline=pipeline,
    inputs={"dest_dir": Path("./my_data")},
    parallel=True
)
```

#### VCF Annotation

```python
from genobear import AnnotationPipelines
from pathlib import Path

# Annotate a VCF file with Ensembl variations
results = AnnotationPipelines.annotate_vcf(
    vcf_path=Path("sample.vcf.gz"),
    output_path=Path("annotated.parquet"),
    variant_type="SNV",
    log=True,
)

# Access the annotated file
annotated_path = results["annotated_vcf_path"]
print(f"Annotated file: {annotated_path}")

# Pre-download reference data
cache_results = AnnotationPipelines.download_ensembl_reference(
    cache_dir=Path("/data/cache"),
    force_download=False,
)
```

## Supported Databases

- **Ensembl**: Ensembl Variation Database (VCF files for all chromosomes)
- **ClinVar**: Clinical Variation Database  
- **dbSNP**: Single Nucleotide Polymorphism Database
- **gnomAD**: Genome Aggregation Database (population genetics)

### Assembly Support
- **GRCh38** (hg38) - Default, modern standard
- **GRCh37** (hg19) - Available for dbSNP

## Configuration

GenoBear uses environment variables for configuration:

```bash
export GENOBEAR_FOLDER="~/genobear"                  # Base folder for all cache/data
export GENOBEAR_DOWNLOAD_WORKERS="8"                 # Number of parallel download workers (default: CPU count)
export GENOBEAR_PARQUET_WORKERS="4"                  # Number of workers for parquet operations - conversion & splitting (default: 4)
export GENOBEAR_WORKERS="4"                          # Number of workers for general processing (default: CPU count)
export GENOBEAR_DOWNLOAD_TIMEOUT="3600"              # Download timeout in seconds (default: 1 hour)
export GENOBEAR_PROGRESS_INTERVAL="10"               # Progress update interval in seconds during downloads (default: 10)
export POLARS_VERBOSE="0"                            # Polars progress output: 0=disabled (clean), 1=enabled (shows rows/s)
export HF_TOKEN="your_huggingface_token"             # HuggingFace token for uploads
```

Create a `.env` file in your project root to set these permanently (see `.env.example`).

## Directory Structure

By default, GenoBear uses platform-specific cache directories (via `platformdirs`):

```
~/.cache/genobear/  (Linux)
~/Library/Caches/genobear/  (macOS)
├── ensembl_vcf/
│   ├── homo_sapiens-chr1.vcf.gz
│   ├── homo_sapiens-chr1.parquet
│   ├── homo_sapiens-chr2.vcf.gz
│   ├── homo_sapiens-chr2.parquet
│   └── ...
├── clinvar/
│   ├── clinvar.vcf.gz
│   └── clinvar.parquet
├── dbsnp_grch38/
│   ├── *.vcf.gz
│   └── *.parquet
└── splitted/
    ├── SNV/
    ├── INS/
    ├── DEL/
    └── ...
```

## Architecture

GenoBear uses a pipeline-based architecture powered by `pipefunc` for composable, parallel genomic data workflows:

### Core Components
- **`Pipelines`** - Static class providing pre-configured pipelines for popular databases
- **Pipeline Functions** - Modular functions for downloading, converting, validating, and splitting VCF data
- **Runtime Execution** - Unified executor configuration with environment-based parallelism controls

### Key Features
1. **Composable Pipelines** - Chain operations: Download → Convert → Validate → Split
2. **Parallel Execution** - Concurrent processing of multiple chromosomes/files
3. **Caching & Validation** - Smart caching with checksum validation and expiry times
4. **Type Safety** - Full type hints and structured logging with Eliot

## Development

### Setup
```bash
# Clone and set up development environment
git clone https://github.com/antonkulaga/genobear.git
cd genobear
uv sync

# Run CLI
uv run genobear --help
```

### Testing Strategy

GenoBear uses a tiered testing approach for comprehensive coverage with practical CI/CD constraints:

#### Test Tiers

**Tier 1: Unit Tests (Always Run)**
- Interface tests, logic validation, error handling
- Fast execution (< 1 second per test)
- No network dependencies

**Tier 2: Integration Tests - Small Downloads (Default)**
- ClinVar tests with manageable file sizes (~50-200MB)
- Full workflow validation: Download → Convert → Annotate
- Reasonable CI time (1-3 minutes)

**Tier 3: Integration Tests - Large Downloads (Manual)**
- dbSNP tests with multi-GB files (3-8GB each)
- Marked with `@pytest.mark.large_download`
- Skipped by default to prevent CI timeouts

#### Test Commands

```bash
# Default test run (Tier 1 + 2)
uv run pytest

# Unit tests only
uv run pytest tests/test_dbsnp_interface.py

# Include large download tests (Tier 3)
uv run pytest -m large_download

# Run all tests including large downloads
uv run pytest -m ""
```

#### Database-Specific Testing

| Database | File Size | Test Strategy |
|----------|-----------|---------------|
| **ClinVar** | 50-200MB | ✅ Default testing |
| **Ensembl** | 10-500MB per chr | ✅ Default testing (selective chromosomes) |
| **dbSNP** | 3-8GB | ❌ Manual testing only |
| **gnomAD** | 1-3GB per chr | 🔄 Manual testing recommended |

### Pipeline API Examples

```python
import genobear as gb

# Quick download with defaults
results = gb.Pipelines.download_clinvar()

# Download with splitting and custom directory
results = gb.Pipelines.download_ensembl(
    dest_dir=Path("./my_data"),
    with_splitting=True
)

# Create custom pipeline with specific steps
pipeline = gb.Pipelines.clinvar(with_splitting=False)
results = gb.Pipelines.execute(
    pipeline=pipeline,
    inputs={"timeout": 7200},
    parallel=True,
    download_workers=8
)

# Split already-downloaded parquet files
results = gb.Pipelines.split_existing_parquets(
    parquet_files=[Path("data.parquet")],
    explode_snv_alt=True
)
```

## Documentation

- [Data Preparation Guide](docs/UPLOAD_HF.md): Detailed guide for preparing and uploading genomic databases
- [VCF Annotation Guide](docs/ANNOTATION.md): Complete guide for annotating VCF files with reference data
- [Agents Guide](AGENTS.md): Guide for AI agents working with GenoBear

## License

MIT License - see LICENSE file for details.