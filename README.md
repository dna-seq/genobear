# GenoBear

A powerful CLI tool for genomic data processing, built on top of the [biobear](https://github.com/wherobots/biobear) library. GenoBear focuses on providing streamlined tools to download and annotate genomic databases and VCF files.

## Features

- **Download genomic databases**: Automatically download and organize popular genomic databases including dbSNP, ClinVar, ANNOVAR, RefSeq, and Exomiser
- **VCF annotation**: Annotate VCF files with multiple genomic databases simultaneously
- **Biobear integration**: Leverages the high-performance biobear library for efficient genomic data processing
- **Async downloads**: Fast, concurrent downloading with progress tracking
- **Flexible configuration**: Environment-configurable defaults for all major settings
- **Rich CLI**: User-friendly command-line interface with comprehensive help and options

## Installation

### Using uv (recommended)

GenoBear is built with uv project management. If you have [uv](https://docs.astral.sh/uv/) installed:

```bash
# Clone the repository
git clone <repository-url>
cd genobear

# Install with uv
uv sync

# Run GenoBear
uv run genobear --help
```

### Using pip

You can also install GenoBear using pip:

```bash
# Install from source
pip install -e .

# Or install dependencies manually
pip install biobear>=0.23.7 typer>=0.16.0 polars>=1.31.0 aiohttp>=3.12.14 rich>=14.1.0
```

## CLI Usage

GenoBear provides two main commands: `download` and `annotate`.

### Basic Usage

```bash
# Show help
genobear --help

# Show version
genobear --version

# Enable verbose output
genobear --verbose <command>
```

### Download Genomic Databases

Download various genomic databases for annotation:

```bash
# Download dbSNP database
genobear download dbsnp --assembly hg38 --release b156

# Download ClinVar database
genobear download clinvar --assembly hg38

# Download multiple databases
genobear download dbsnp clinvar --assembly hg38

# Download with custom output directory
genobear download dbsnp --assembly hg38 --folder ~/my_databases/dbsnp

# List available download commands
genobear download --help
```

### Annotate VCF Files

Annotate VCF files with downloaded databases:

```bash
# Annotate with dbSNP
genobear annotate vcf input.vcf --databases dbsnp --assembly hg38

# Annotate with multiple databases
genobear annotate vcf input.vcf --databases dbsnp clinvar --assembly hg38

# Specify custom database and output folders
genobear annotate vcf input.vcf \
  --databases dbsnp \
  --databases-folder ~/my_databases \
  --output-folder ~/annotations

# List available annotation commands
genobear annotate --help
```

## Supported Databases

- **dbSNP**: Single Nucleotide Polymorphism Database
- **ClinVar**: Clinical Variation Database
- **dbNSFP**: Database for Non-synonymous SNPs
- **HGMD**: Human Gene Mutation Database
- **ANNOVAR**: Functional annotation databases
- **RefSeq**: Reference Sequence Database
- **Exomiser**: Genomic variant prioritization tool databases

## Configuration

GenoBear supports environment-based configuration. Set these environment variables to customize default behavior:

```bash
# Database URLs and folders
export GENOBEAR_DBSNP_URL="https://ftp.ncbi.nih.gov/snp/archive"
export GENOBEAR_DBSNP_FOLDER="~/genobear/databases/dbsnp"
export GENOBEAR_CLINVAR_FOLDER="~/genobear/databases/clinvar"

# Download settings
export GENOBEAR_MAX_CONCURRENT_DOWNLOADS="3"
export GENOBEAR_DEFAULT_ASSEMBLY="hg38"
export GENOBEAR_DEFAULT_DBSNP_RELEASE="b156"

# Processing settings
export GENOBEAR_PARQUET_BATCH_SIZE="100000"
```

## Dependencies

GenoBear is built on top of several powerful libraries:

- **[biobear](https://github.com/wherobots/biobear)** (≥0.23.7): High-performance library for bioinformatics data processing
- **[typer](https://typer.tiangolo.com/)** (≥0.16.0): Modern CLI framework
- **[polars](https://pola.rs/)** (≥1.31.0): Fast DataFrame library
- **[aiohttp](https://docs.aiohttp.org/)** (≥3.12.14): Async HTTP client for downloads
- **[rich](https://rich.readthedocs.io/)** (≥14.1.0): Rich text and beautiful formatting

## Development

This project uses:
- **uv** for dependency management
- **Eliot** for structured logging
- **Type hints** throughout the codebase
- **Async/await** for concurrent operations

## License

See LICENSE file for details.
