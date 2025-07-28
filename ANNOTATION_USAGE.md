# GenoBear VCF Annotation

GenoBear now provides powerful VCF annotation capabilities using the same databases that can be downloaded via the `download` command. The annotation functionality uses streaming processing with biobear to handle large files efficiently without loading everything into memory.

## Installation

Make sure you have genobear installed with all dependencies:

```bash
# Install with uv (recommended)
git clone <repository>
cd genobear
uv sync

# Or with pip
pip install -e .
```

## Quick Start

### 1. Download Annotation Databases

First, download some annotation databases:

```bash
# Download dbSNP for hg38
python -m genobear download dbsnp hg38 --parquet

# Download ClinVar for hg38  
python -m genobear download clinvar hg38 --parquet

# Download dbNSFP (requires configuration)
# python -m genobear download dbnsfp hg38
```

### 2. List Available Databases

Check what annotation databases are available:

```bash
# List all databases
python -m genobear annotate list

# List specific database type
python -m genobear annotate list --type dbsnp --assembly hg38

# List for specific assembly
python -m genobear annotate list --assembly hg38
```

### 3. Annotate VCF Files

#### Single Database Annotation

Annotate with a single database:

```bash
# Basic annotation with dbSNP
python -m genobear annotate single input.vcf.gz ~/genobear/databases/dbsnp/hg38/latest/dbsnp.parquet

# Specify output file
python -m genobear annotate single input.vcf.gz dbsnp.parquet --output annotated_output.parquet

# Custom join columns (default: chrom,pos,ref,alt)
python -m genobear annotate single input.vcf.gz dbsnp.parquet --join-on "chrom,pos,ref,alt"
```

#### Multi-Database Annotation

Annotate with multiple databases automatically:

```bash
# Annotate with all available databases for hg38
python -m genobear annotate multi input.vcf.gz --assembly hg38

# Annotate with specific database types
python -m genobear annotate multi input.vcf.gz --types dbsnp,clinvar --assembly hg38

# Specify custom databases folder
python -m genobear annotate multi input.vcf.gz --databases-folder /path/to/databases --assembly hg38
```

#### Batch Processing

Process multiple VCF files at once:

```bash
# Process all VCF files in current directory
python -m genobear annotate batch *.vcf.gz --assembly hg38

# Process specific files with custom output folder
python -m genobear annotate batch file1.vcf.gz file2.vcf.gz --output-folder /path/to/output --assembly hg38

# Control concurrency
python -m genobear annotate batch *.vcf.gz --assembly hg38 --max-concurrent 4
```

## Advanced Usage

### Memory-Efficient Processing

The annotation functionality is designed for memory efficiency:

- **Streaming Processing**: Uses biobear's streaming capabilities to process data in batches
- **Configurable Batch Size**: Control memory usage with `--batch-size` parameter
- **SQL-Based Joins**: Efficient SQL joins handled by the underlying query engine

### Join Column Configuration

By default, annotation joins on `chrom,pos,ref,alt`. You can customize this:

```bash
# Different join columns
python -m genobear annotate single input.vcf.gz dbsnp.parquet --join-on "chromosome,position,reference,alternate"

# Minimal join (position only, less specific)
python -m genobear annotate single input.vcf.gz dbsnp.parquet --join-on "chrom,pos"
```

### Output Formats

- **Parquet** (default): Most efficient for further processing
- **TSV**: When output file doesn't end in `.parquet`, outputs tab-separated values

### Error Handling

- Files are validated before processing
- Graceful handling of missing databases
- Detailed logging with eliot for troubleshooting

## Environment Variables

Configure default paths via environment variables:

```bash
# Set default databases folder
export GENOBEAR_DATABASES_FOLDER="/path/to/your/databases"

# Set default output folder
export GENOBEAR_OUTPUT_FOLDER="/path/to/your/output"

# Control batch size for memory usage
export GENOBEAR_PARQUET_BATCH_SIZE="50000"

# Control concurrency
export GENOBEAR_MAX_CONCURRENT_ANNOTATIONS="4"
```

## Example Workflow

Complete workflow from download to annotation:

```bash
# 1. Download databases
python -m genobear download dbsnp hg38 --parquet
python -m genobear download clinvar hg38 --parquet

# 2. Check available databases
python -m genobear annotate list --assembly hg38

# 3. Annotate your VCF
python -m genobear annotate multi my_variants.vcf.gz --assembly hg38

# 4. Check output
ls -la my_variants.multi_annotated.parquet
```

## Performance Notes

- **Streaming**: Processes files of any size without loading into memory
- **Parallelization**: Batch mode supports concurrent processing
- **Efficient Storage**: Parquet format provides excellent compression and query performance
- **SQL Engine**: Leverages DataFusion for optimized query execution

## Troubleshooting

### Common Issues

1. **No databases found**: Make sure you've downloaded databases first with the `download` command
2. **Memory issues**: Reduce `--batch-size` for very large files
3. **Join mismatches**: Verify your VCF and annotation files use compatible column naming

### Logging

GenoBear uses structured logging. Check the logs directory for detailed information:

```bash
# View JSON logs
cat logs/genobear.log.json | jq .

# View human-readable logs  
cat logs/genobear.log
```

## Integration with Other Tools

The output parquet files can be easily used with:

- **Polars**: `df = pl.read_parquet("annotated.parquet")`
- **Pandas**: `df = pd.read_parquet("annotated.parquet")`
- **DuckDB**: `SELECT * FROM 'annotated.parquet'`
- **BioBear**: `session.sql("SELECT * FROM read_parquet('annotated.parquet')")`