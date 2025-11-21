# GenoBear Annotation Pipeline

## Overview

The GenoBear annotation pipeline allows you to annotate VCF files with reference genomic data from Ensembl variations. The pipeline is built using `pipefunc` for efficient parallel processing and lazy evaluation with Polars.

## Key Features

- **Automatic cache management**: Downloads reference data from HuggingFace Hub only when needed
- **Lazy evaluation**: Uses Polars LazyFrames for memory-efficient processing
- **Parallel annotation**: Processes multiple chromosomes in parallel
- **Pipeline-based**: Built on pipefunc for caching, composition, and parallel execution

## Installation

```bash
uv sync
```

## Quick Start

### Basic VCF Annotation

```bash
# Annotate a VCF file with default settings
annotate vcf input.vcf.gz -o annotated.parquet

# The pipeline will:
# 1. Check if ensembl_variations cache exists (~/.cache/genobear/ensembl_variations/splitted_variants)
# 2. Download reference data if not cached (only on first run)
# 3. Parse the VCF to identify chromosomes
# 4. Perform lazy joins with reference data for each chromosome
# 5. Save the annotated result to parquet
```

### Download Reference Data

```bash
# Pre-download the reference data to default cache location
annotate download-reference

# Download to a custom location
annotate download-reference --cache-dir /path/to/cache

# Force re-download
annotate download-reference --force
```

## CLI Commands

### `annotate vcf`

Annotate a VCF file with ensembl_variations data.

**Usage:**
```bash
annotate vcf [OPTIONS] VCF_PATH
```

**Options:**
- `-o, --output PATH`: Output path for annotated parquet file (default: `data/output/<vcf_name>_annotated.parquet`)
- `--cache-dir PATH`: Cache directory for ensembl_variations data (default: uses `GENOBEAR_CACHE_DIR` env or platform-specific cache)
- `--repo-id TEXT`: HuggingFace repository ID (default: `just-dna-seq/ensembl_variations`)
- `--variant-type TEXT`: Variant type to use for annotation (default: `SNV`)
- `--token TEXT`: HuggingFace API token (uses `HF_TOKEN` env variable if not specified)
- `--force-download`: Force re-download of reference data even if cache exists
- `--workers INT`: Number of workers for parallel processing
- `--log/--no-log`: Enable detailed logging to files (default: enabled)
- `--run-folder PATH`: Optional run folder for pipeline execution and caching

**Examples:**

```bash
# Basic annotation
annotate vcf sample.vcf.gz -o annotated.parquet

# Annotate with custom cache directory
annotate vcf sample.vcf.gz -o output.parquet --cache-dir /data/cache

# Force re-download of reference data
annotate vcf sample.vcf.gz --force-download

# Use specific variant type
annotate vcf sample.vcf.gz --variant-type INDEL

# Disable logging for cleaner output
annotate vcf sample.vcf.gz --no-log
```

### `annotate download-reference`

Download ensembl_variations reference data from HuggingFace Hub.

**Usage:**
```bash
annotate download-reference [OPTIONS]
```

**Options:**
- `--cache-dir PATH`: Cache directory for data (default: uses `GENOBEAR_CACHE_DIR` env or platform-specific cache)
- `--repo-id TEXT`: HuggingFace repository ID (default: `just-dna-seq/ensembl_variations`)
- `--token TEXT`: HuggingFace API token
- `--force/--no-force`: Force re-download even if cache exists
- `--log/--no-log`: Enable detailed logging to files

**Examples:**

```bash
# Download to default location
annotate download-reference

# Download to custom location
annotate download-reference --cache-dir /data/ensembl_cache

# Force re-download
annotate download-reference --force
```

## Python API

You can also use the annotation pipeline programmatically:

```python
from pathlib import Path
from genobear import AnnotationPipelines

# Annotate a VCF file
results = AnnotationPipelines.annotate_vcf(
    vcf_path=Path("sample.vcf.gz"),
    output_path=Path("annotated.parquet"),
    variant_type="SNV",
    log=True,
)

# Access the annotated file path
annotated_path = results["annotated_vcf_path"]
print(f"Annotated file: {annotated_path}")

# Download reference data
cache_results = AnnotationPipelines.download_ensembl_reference(
    cache_dir=Path("/data/cache"),
    force_download=False,
)

cache_path = cache_results["ensembl_cache_path"]
print(f"Reference data at: {cache_path}")
```

### Low-Level Pipeline Components

```python
from genobear.pipelines.annotation import (
    download_ensembl_from_hf,
    extract_chromosomes_from_vcf,
    load_vcf_as_lazy_frame,
    find_annotation_parquet_files,
    annotate_vcf_with_ensembl,
    save_annotated_vcf,
)

# Download reference data
cache_path = download_ensembl_from_hf(
    repo_id="just-dna-seq/ensembl_variations",
    cache_dir=None,  # Uses default
)

# Load VCF as LazyFrame
vcf_lf = load_vcf_as_lazy_frame("sample.vcf.gz")

# Extract chromosomes
chromosomes = extract_chromosomes_from_vcf("sample.vcf.gz")
print(f"Found chromosomes: {chromosomes}")

# Find annotation files
parquet_paths = find_annotation_parquet_files(
    ensembl_cache_path=cache_path,
    chromosomes=chromosomes,
    variant_type="SNV",
)

# Annotate VCF
annotated_lf = annotate_vcf_with_ensembl(
    vcf_lazy_frame=vcf_lf,
    annotation_parquet_paths=parquet_paths,
)

# Save result
output_path = save_annotated_vcf(
    annotated_vcf_lazy=annotated_lf,
    output_path=Path("annotated.parquet"),
)
```

## Architecture

The annotation pipeline consists of several components:

### 1. HF Dataset Downloader (`hf_dataset_downloader.py`)
- Downloads ensembl_variations dataset from HuggingFace Hub
- Checks if cache exists and skips download if present (unless forced)
- Uses `snapshot_download` to fetch parquet files

### 2. VCF Parser (`vcf_parser.py`)
- Extracts chromosome identifiers from input VCF
- Loads VCF as Polars LazyFrame for efficient processing
- Handles both compressed (.vcf.gz) and uncompressed (.vcf) files

### 3. Annotator (`annotator.py`)
- Finds annotation parquet files for each chromosome
- Performs lazy left joins between VCF and reference data
- Supports multiple join columns for precise matching
- Saves annotated results with compression

### 4. Pipeline Runners (`runners.py`)
- Provides high-level `AnnotationPipelines` class
- Composes individual functions into complete pipelines
- Handles execution with proper configuration
- Manages parallel processing and caching

## Cache Structure

The reference data is organized by variant type. By default, data is cached at:
- Linux: `~/.cache/genobear/ensembl_variations/splitted_variants/`
- macOS: `~/Library/Caches/genobear/ensembl_variations/splitted_variants/`
- Windows: `%LOCALAPPDATA%\genobear\Cache\ensembl_variations\splitted_variants\`

You can override the base cache directory with the `GENOBEAR_CACHE_DIR` environment variable:

```bash
export GENOBEAR_CACHE_DIR=/data/genobear_cache
annotate vcf input.vcf.gz -o annotated.parquet
```

Directory structure:
```
${CACHE_DIR}/ensembl_variations/splitted_variants/
└── data/
    ├── SNV/
    │   ├── homo_sapiens-chr1.parquet
    │   ├── homo_sapiens-chr2.parquet
    │   └── ...
    ├── INDEL/
    │   └── ...
    └── ...
```

## Join Strategy

The annotation pipeline uses a **left join** strategy to preserve all variants from the input VCF:

```python
join_columns = ["chromosome", "start", "reference", "alternate"]
```

This ensures:
- All input variants are retained
- Matching reference data is added where available
- Non-matching variants have `null` values for reference columns

## Performance

The pipeline is optimized for performance:

- **Lazy evaluation**: Uses Polars LazyFrames to minimize memory usage
- **Parallel processing**: Annotates multiple chromosomes simultaneously
- **Streaming writes**: Uses `sink_parquet` for memory-efficient output
- **Caching**: Reference data is downloaded once and reused
- **Compression**: Output files use zstd compression by default

## Environment Variables

- `GENOBEAR_CACHE_DIR`: Override the base cache directory (default: `~/.cache/genobear` on Linux, varies by platform)
- `GENOBEAR_WORKERS`: Number of workers for parallel processing (default: CPU count)
- `HF_TOKEN`: HuggingFace API token for accessing datasets
- `POLARS_VERBOSE`: Polars logging level (default: 0 for clean output)

## Logging

When logging is enabled, detailed logs are written to:
- `logs/annotate_vcf.json`: JSON-formatted structured logs
- `logs/annotate_vcf.log`: Human-readable logs

Use Eliot tools to analyze JSON logs:
```bash
python -m eliot.tree logs/annotate_vcf.json
```

## Testing

Run the integration tests:

```bash
# Run annotation pipeline tests
pytest tests/test_annotation_pipeline_integration.py -vvv

# Skip large download tests
pytest tests/test_annotation_pipeline_integration.py -vvv -m "not large_download"
```

## Troubleshooting

### Reference data not found
- Ensure you have internet access on first run
- Check HuggingFace Hub status
- Try force re-download: `annotate download-reference --force`

### Memory issues
- The pipeline uses lazy evaluation to minimize memory usage
- Adjust `--workers` to reduce parallel processing
- Ensure sufficient disk space for output files

### Slow performance
- Reference data download is slow on first run (several GB)
- Subsequent runs use cached data and are much faster
- Consider pre-downloading: `annotate download-reference`

## Related Commands

See also:
- `prepare ensembl`: Download and prepare Ensembl VCF data
- `prepare clinvar`: Download and prepare ClinVar data

