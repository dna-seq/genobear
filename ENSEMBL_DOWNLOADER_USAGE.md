# Ensembl Variants Downloader Usage

The `EnsemblVariantsDownloader` provides access to Ensembl's refined variant annotations from their FTP server. It supports multiple types of variant files and follows the same pattern as other downloaders in the genobear package.

## Available Variant Types

- **`clinically_associated`** - Clinically associated variants (3.8M)
- **`phenotype_associated`** - Phenotype associated variants (41M) **[Default]**
- **`somatic`** - Somatic variants (111M)
- **`somatic_incl_consequences`** - Somatic variants with consequence annotations (160M)
- **`structural_variations`** - Structural variations (264M)
- **`chromosome_specific`** - Individual chromosome files (e.g., chr1, chrX)
- **`1000genomes`** - 1000 Genomes Phase 3 data (1.5G)

## Python API Usage

### Basic Usage

```python
from genobear import EnsemblVariantsDownloader, EnsemblVariantType

# Download phenotype-associated variants (default)
downloader = EnsemblVariantsDownloader()
vcf_path = downloader.vcf_path  # Downloads automatically

# Read the VCF file  
df = downloader.read_vcf()
```

### Different Variant Types

```python
# Clinically associated variants
clinical_downloader = EnsemblVariantsDownloader(
    variant_type=EnsemblVariantType.CLINICALLY_ASSOCIATED
)

# Somatic variants with consequences (includes VEP annotations)
somatic_downloader = EnsemblVariantsDownloader(
    variant_type=EnsemblVariantType.SOMATIC_WITH_CONSEQUENCES
)

# 1000 Genomes Phase 3
genomes_downloader = EnsemblVariantsDownloader(
    variant_type=EnsemblVariantType.THOUSAND_GENOMES
)
```

### Chromosome-Specific Downloads

```python
# Download specific chromosome
chr21_downloader = EnsemblVariantsDownloader(
    variant_type=EnsemblVariantType.CHROMOSOME_SPECIFIC,
    chromosome="21"
)

# Download multiple chromosomes
downloader = EnsemblVariantsDownloader()
chromosome_paths = downloader.download_all_chromosomes(
    chromosomes=["1", "2", "X", "Y"]  # or None for all chromosomes
)
```

## Command Line Usage

### Download Commands

```bash
# Download phenotype-associated variants (default)
uv run python -m genobear.downloaders.ensembl_variants_downloader download

# Download clinically associated variants
uv run python -m genobear.downloaders.ensembl_variants_downloader download \
    --variant-type clinically_associated

# Download somatic variants with consequences
uv run python -m genobear.downloaders.ensembl_variants_downloader download \
    --variant-type somatic_incl_consequences

# Download specific chromosome
uv run python -m genobear.downloaders.ensembl_variants_downloader download \
    --variant-type chromosome_specific --chromosome 21

# Download with custom cache directory
uv run python -m genobear.downloaders.ensembl_variants_downloader download \
    --cache-subdir ./my_ensembl_cache --verbose
```

### Download All Chromosomes

```bash
# Download all chromosomes (1-22, X, Y, MT)
uv run python -m genobear.downloaders.ensembl_variants_downloader download-all-chromosomes

# Download specific chromosomes only
uv run python -m genobear.downloaders.ensembl_variants_downloader download-all-chromosomes \
    --chromosomes "1,2,3,X,Y"

# Download with options
uv run python -m genobear.downloaders.ensembl_variants_downloader download-all-chromosomes \
    --no-decompress --force --verbose
```

## File Formats and Annotations

### Index Files
Ensembl uses `.csi` (Coordinate-Sorted Index) files instead of `.tbi` files. These are automatically downloaded and handled by the downloader.

### Annotations Included
Ensembl VCF files include refined annotations such as:
- **Gene symbols and IDs** 
- **Transcript consequences** (missense, synonymous, etc.)
- **Variant Effect Predictor (VEP) annotations** (in consequence files)
- **Population frequency data**
- **Clinical significance** (in clinical files)
- **Phenotype associations** (in phenotype files)

### File Sizes (Approximate)
- Clinically associated: 3.8MB
- Phenotype associated: 41MB  
- Somatic: 111MB
- Somatic with consequences: 160MB
- Structural variations: 264MB
- Individual chromosomes: 300MB-1GB each
- 1000 Genomes: 1.5GB

## Configuration Options

```python
downloader = EnsemblVariantsDownloader(
    variant_type=EnsemblVariantType.PHENOTYPE_ASSOCIATED,
    cache_subdir="custom/ensembl/cache",  # Custom cache location
    base_url="https://custom-mirror.org/",  # Custom mirror
    streaming=True  # Enable streaming for large files
)
```

## Integration with Annotations

The downloaded Ensembl VCF files can be used directly with genobear's annotation system:

```python
from genobear import VCFAnnotator

# Download Ensembl variants
ensembl_downloader = EnsemblVariantsDownloader(
    variant_type=EnsemblVariantType.SOMATIC_WITH_CONSEQUENCES
)

# Create annotator using the downloaded file
annotator = VCFAnnotator(ensembl_downloader.vcf_path)

# Annotate your VCF
my_vcf_path = "path/to/my/variants.vcf"
annotated_df = annotator.annotate(my_vcf_path)
```
