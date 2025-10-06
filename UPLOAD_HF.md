# Uploading to Hugging Face

This document describes how to upload genomic data parquet files (Ensembl variations, ClinVar) to Hugging Face Hub datasets.

## Overview

The upload pipeline intelligently uploads parquet files to Hugging Face Hub by:
1. Comparing local file sizes with remote file sizes
2. Only uploading files that differ in size
3. Supporting parallel uploads for efficiency
4. Providing detailed logging and progress tracking
5. Preserving directory structure (e.g., split variants by type: `deletion/`, `SNV/`, `indel/`, etc.)

## Prerequisites

### Authentication

You need a Hugging Face token with write permissions. You can provide it in two ways:

1. **Environment Variable** (recommended):
```bash
export HF_TOKEN="your_token_here"
```

2. **Command-line option**:
```bash
prepare upload-ensembl --token "your_token_here"
```

### Repository Access

You need write access to the target repositories:
- Ensembl: `just-dna-seq/ensembl_variations` 
- ClinVar: `just-dna-seq/clinvar`

## Usage

### Command Line Interface

#### Ensembl Variations

**Basic Usage (Upload from default cache):**
```bash
prepare upload-ensembl
```

**Upload from Custom Directory:**
```bash
prepare upload-ensembl --source-dir /path/to/parquet/files
```

**Combined Download and Upload:**
```bash
# Download, split by variant type, and immediately upload to Hugging Face
prepare ensembl --split --upload

# Download to custom directory and upload to custom repo
prepare ensembl --dest-dir /data/ensembl --split --upload --repo-id username/my-ensembl
```

**Upload to Different Repository:**
```bash
prepare upload-ensembl --repo-id username/my-dataset
```

**With Custom Pattern:**
```bash
# Upload only SNV files
prepare upload-ensembl --pattern "**/SNV/*.parquet"

# Upload only chromosome 21 files
prepare upload-ensembl --pattern "**/*chr21*.parquet"
```

**With Custom Workers:**
```bash
prepare upload-ensembl --workers 8
```

#### ClinVar

**Basic Usage (Upload from default cache):**
```bash
prepare upload-clinvar
```

**Upload from Custom Directory:**
```bash
prepare upload-clinvar --source-dir /path/to/parquet/files
```

**Combined Download and Upload:**
```bash
# Download and immediately upload to Hugging Face
prepare clinvar --split --upload

# Download to custom directory and upload to custom repo
prepare clinvar --dest-dir /data/clinvar --upload --repo-id username/my-clinvar
```

**With Custom Repository:**
```bash
prepare upload-clinvar --repo-id username/my-dataset
```

### Python API

```python
from genobear import PreparationPipelines
from pathlib import Path

# Ensembl: Basic upload from default cache
results = PreparationPipelines.upload_ensembl_to_hf()

# Ensembl: Upload from custom directory
results = PreparationPipelines.upload_ensembl_to_hf(
    source_dir=Path("/path/to/parquet/files"),
    repo_id="just-dna-seq/ensembl_variations",
    token="your_token_here",
    workers=8
)

# ClinVar: Basic upload from default cache
results = PreparationPipelines.upload_clinvar_to_hf()

# ClinVar: Upload from custom directory
results = PreparationPipelines.upload_clinvar_to_hf(
    source_dir=Path("/path/to/clinvar"),
    repo_id="just-dna-seq/clinvar",
    token="your_token_here",
    workers=4
)

# Check results
uploaded_files = results["uploaded_files"]
for file_info in uploaded_files.output.ravel().tolist():
    print(f"{file_info['file']}: uploaded={file_info['uploaded']}, reason={file_info['reason']}")
```

### Low-Level API

```python
from genobear.pipelines.huggingface_uploader import upload_parquet_to_hf
from pathlib import Path

# Collect your parquet files
parquet_files = list(Path("/path/to/data").glob("*.parquet"))

# Upload with size comparison
results = upload_parquet_to_hf(
    parquet_files=parquet_files,
    repo_id="just-dna-seq/ensembl_variations",
    token="your_token_here",
    path_prefix="data",
    parallel=True,
    workers=4
)
```

## Command-line Options

### Upload Commands (`upload-ensembl`, `upload-clinvar`)

| Option | Default | Description |
|--------|---------|-------------|
| `--source-dir` | Default cache | Source directory containing parquet files |
| `--repo-id` | `just-dna-seq/ensembl_variations` or `just-dna-seq/clinvar` | Hugging Face repository ID |
| `--token` | `$HF_TOKEN` | Hugging Face API token |
| `--pattern` | `**/*.parquet` | Glob pattern for finding files |
| `--path-prefix` | `data` | Prefix for paths in the repository |
| `--workers` | Auto (CPU count) | Number of parallel upload workers |
| `--log/--no-log` | `--log` | Enable detailed logging |

### Download with Upload (`clinvar`)

Additional options for `prepare clinvar` command:

| Option | Default | Description |
|--------|---------|-------------|
| `--upload/--no-upload` | `--no-upload` | Upload parquet files after download |
| `--repo-id` | `just-dna-seq/clinvar` | Hugging Face repository ID for upload |
| `--token` | `$HF_TOKEN` | Hugging Face API token |

## Output

The command provides:

1. **Progress Display**: Real-time upload status with rich formatting
2. **Summary Statistics**:
   - Total files processed
   - Files uploaded (new or size changed)
   - Files skipped (size match)
3. **Detailed Logs**: JSON and text logs in the `logs/` directory

### Example Output

```
🔧 Setting up Hugging Face upload pipeline...
📦 Repository: just-dna-seq/ensembl_variations
📁 Source: default cache location
🔍 Pattern: **/*.parquet
👷 Workers: 8
🚀 Starting upload...
✅ Upload completed

✅ Upload process completed!
📊 Summary:
  - Total files: 25
  - Uploaded: 3
  - Skipped (size match): 22
```

## How It Works

### Size Comparison Logic

For each file:
1. Calculate local file size
2. Query remote file size from Hugging Face Hub
3. Compare sizes:
   - **Match**: Skip upload (file already up to date)
   - **Differ**: Upload file with commit message
   - **New**: Upload file (file doesn't exist remotely)

### Upload Strategy

- Files are uploaded in parallel using multiple workers
- Each file is uploaded independently
- Failures on individual files don't stop the entire process
- Detailed logging tracks success and failures

## Integration with Download Pipeline

Complete workflow examples:

### Ensembl (Two-Step)
```bash
# 1. Download and process Ensembl data
prepare ensembl --split --pattern "chr(21|22)"

# 2. Upload to Hugging Face
prepare upload-ensembl

# 3. Check logs
cat logs/upload_ensembl.log
```

### ClinVar (One-Step)
```bash
# Download, process, and upload in one command
prepare clinvar --split --upload

# Check logs
cat logs/prepare_clinvar.log
cat logs/upload_clinvar.log
```

### ClinVar (Two-Step)
```bash
# 1. Download and process ClinVar data
prepare clinvar --split

# 2. Upload to Hugging Face separately
prepare upload-clinvar

# Check logs
cat logs/upload_clinvar.log
```

## Troubleshooting

### Authentication Errors

If you get authentication errors:
```bash
# Login to Hugging Face CLI
huggingface-cli login

# Or set token explicitly
export HF_TOKEN="your_token_here"
```

### Permission Errors

Ensure you have write access to the repository. For `just-dna-seq/ensembl_variations`, you need to be a collaborator.

### Network Errors

The pipeline will log errors but continue with other files. Check `logs/upload_ensembl_hf.log` for details.

### Large Files

For very large files, consider:
- Increasing the timeout (if added as option)
- Using fewer workers to reduce memory pressure
- Uploading in batches with custom patterns

## Environment Variables

| Variable | Description |
|----------|-------------|
| `HF_TOKEN` | Hugging Face API token |
| `GENOBEAR_WORKERS` | Default number of workers |
| `GENOBEAR_FOLDER` | Base folder for GenoBear data |

## Notes

- The upload pipeline uses the `huggingface_hub` library
- File comparison is based on size only (not content hash)
- Commit messages are automatically generated
- All uploads are tracked in eliot logs for full traceability

