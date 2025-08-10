# GenoBear 🧬

A unified toolkit for downloading, converting, and annotating genomic databases.

## Features

- **Download genomic databases**: dbSNP, ClinVar, ANNOVAR, RefSeq, Exomiser, and HGMD
- **Convert VCF to Parquet**: Efficient columnar storage for large genomic datasets  
- **Annotate VCF files**: Add variant annotations from multiple databases
- **CLI and Python API**: Use via command line or import as a Python library
- **Unified configuration**: Consistent database paths and discovery
- **Streaming processing**: Handle large files efficiently with biobear

## Installation

```bash
# Using uv (recommended)
uv add genobear

# Using pip
pip install genobear
```

## Quick Start

### CLI Usage

```bash
# Download and convert dbSNP database for hg38
genobear download dbsnp hg38 --parquet

# Download ClinVar database  
genobear download clinvar hg38 --parquet

# List available databases
genobear annotate list

# Annotate a VCF file
genobear annotate single input.vcf.gz --assembly hg38

# Batch annotate multiple VCF files
genobear annotate batch *.vcf.gz --assembly hg38
```

### Python API Usage

```python
import genobear as gb
from pathlib import Path

# Download and convert databases (defaults to hg38)
gb.download_and_convert_dbsnp()  # Downloads hg38, release b156
gb.download_and_convert_clinvar()  # Downloads hg38

# For specific assemblies
gb.download_and_convert_dbsnp(assemblies=["hg19"], releases=["b156"])
gb.download_and_convert_clinvar(assemblies=["hg19"])

# Annotate VCF files
vcf_file = Path("input.vcf.gz")
result = gb.annotate_vcf(
    vcf_path=vcf_file,
    database_types=["dbsnp", "clinvar"],
    assembly="hg38"
)

# Discover available databases
databases = gb.discover_databases(assembly="hg38")
print(f"Available databases: {list(databases.keys())}")
```

## Supported Databases

- **dbSNP**: Single Nucleotide Polymorphism Database
- **ClinVar**: Clinical Variation Database  
- **ANNOVAR**: Functional annotation databases
- **RefSeq**: Reference Sequence Database
- **Exomiser**: Variant prioritization tool
- **HGMD**: Human Gene Mutation Database (license required)

### Assembly Support
- **hg38** (GRCh38) - Default
- **hg19** (GRCh37) - Legacy support

## Configuration

GenoBear uses environment variables for configuration:

```bash
export GENOBEAR_BASE_FOLDER="~/genobear"              # Base folder for all data
export GENOBEAR_DEFAULT_ASSEMBLY="hg38"              # Default genome assembly  
export GENOBEAR_DEFAULT_DBSNP_RELEASE="b156"         # Default dbSNP release
export GENOBEAR_MAX_CONCURRENT_DOWNLOADS="3"         # Concurrent downloads
export GENOBEAR_PARQUET_BATCH_SIZE="100000"          # Parquet conversion batch size
```

## Directory Structure

```
~/genobear/
├── databases/
│   ├── dbsnp/
│   │   └── hg38/
│   │       └── b156/
│   │           ├── *.vcf.gz
│   │           └── dbsnp_hg38_b156.parquet
│   ├── clinvar/
│   │   └── hg38/
│   │       └── latest/
│   │           ├── *.vcf.gz  
│   │           └── clinvar_hg38.parquet
│   └── ...
├── output/
│   ├── vcf_files/
│   │   ├── sample1.annotated.vcf
│   │   └── sample2.annotated.vcf
│   └── *.annotated.vcf
└── logs/
    └── genobear.log
```

## Architecture

GenoBear features a clean, refactored architecture that eliminates code duplication:

### Base Classes
- **`DatabaseDownloader`** - Abstract base class for all database downloaders
- **`VcfDatabaseDownloader`** - Base class for VCF-based databases (dbSNP, ClinVar)  
- **`ArchiveDatabaseDownloader`** - Base class for archive-based databases (ANNOVAR, Exomiser)

### Key Improvements
1. **Assembly-Specific Downloaders** - Each downloader handles one assembly
2. **Default to hg38** - Modern standard as default
3. **Simplified APIs** - No factory pattern, direct instantiation
4. **Cleaner Interface** - Assembly as a simple parameter

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
| **dbSNP** | 3-8GB | ❌ Manual testing only |
| **ANNOVAR** | Varies | 🔄 Evaluate per dataset |
| **Exomiser** | 1-3GB | 🔄 Consider manual only |

### Migration from Legacy Code

For new code, use the simplified interface:
```python
from genobear.databases.clinvar import ClinVarDownloader, download_clinvar
from genobear.databases.dbsnp import DbSnpDownloader, download_dbsnp

# Simple function interface (defaults to hg38)
results = await download_clinvar()
results = await download_dbsnp()

# Class interface for more control  
downloader = ClinVarDownloader(assembly="hg38")
results = await downloader.download()
```

## License

MIT License - see LICENSE file for details.