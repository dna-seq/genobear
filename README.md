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

# Download and convert databases
gb.download_and_convert_dbsnp(assemblies=["hg38"], releases=["b156"])
gb.download_and_convert_clinvar(assemblies=["hg38"])

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

## Development

```bash
# Clone and set up development environment
git clone https://github.com/antonkulaga/genobear.git
cd genobear
uv sync

# Run CLI
uv run genobear --help

# Run tests
uv run pytest
```

## License

MIT License - see LICENSE file for details.