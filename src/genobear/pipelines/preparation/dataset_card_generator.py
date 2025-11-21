"""
Dataset card generator for HuggingFace Hub.

This module generates README.md dataset cards for genomic datasets.
Supports both template-based generation (from .md files) and programmatic generation.
"""

from pathlib import Path
from typing import Optional, Dict, List
from datetime import datetime


def load_template(template_name: str) -> Optional[str]:
    """
    Load a dataset card template from the dataset_cards directory.
    
    Args:
        template_name: Name of the template file (e.g., 'ensembl_card_template.md')
        
    Returns:
        Template content as string, or None if not found
    """
    # Try to find template in dataset_cards directory relative to project root
    current_file = Path(__file__)
    project_root = current_file.parent.parent.parent.parent.parent
    template_path = project_root / "dataset_cards" / template_name
    
    if template_path.exists():
        return template_path.read_text()
    
    return None


def render_template(
    template_content: str,
    variables: Dict[str, str]
) -> str:
    """
    Render a template by replacing {{variable}} placeholders.
    
    Args:
        template_content: Template string with {{placeholders}}
        variables: Dictionary of variable names to values
        
    Returns:
        Rendered template string
    """
    result = template_content
    for key, value in variables.items():
        placeholder = f"{{{{{key}}}}}"
        result = result.replace(placeholder, str(value))
    return result


def generate_ensembl_card(
    num_files: int,
    total_size_gb: float,
    variant_types: Optional[List[str]] = None,
    version: Optional[str] = None,
    use_template: bool = True,
) -> str:
    """
    Generate a dataset card for Ensembl variations dataset.
    
    Args:
        num_files: Total number of parquet files in the dataset
        total_size_gb: Total size of the dataset in GB
        variant_types: List of variant types (SNV, deletion, indel, etc.)
        version: Ensembl version or date
        use_template: Whether to use template file (if available) or generate programmatically
        
    Returns:
        Markdown content for the dataset card
    """
    date_str = datetime.now().strftime("%Y-%m-%d")
    current_year = datetime.now().year
    
    # Prepare variant types section
    variant_types_section = ""
    if variant_types:
        variant_list = "\n".join(f"- `{vt}/`" for vt in sorted(variant_types))
        variant_types_section = f"""
## Dataset Structure

The dataset is organized by variant type:

{variant_list}

Each directory contains parquet files split by chromosome.
"""
    
    # Try to load template if requested
    if use_template:
        template = load_template("ensembl_card_template.md")
        if template:
            variables = {
                "update_date": date_str,
                "num_files": str(num_files),
                "total_size_gb": f"{total_size_gb:.1f}",
                "variant_types_section": variant_types_section,
                "current_year": str(current_year),
            }
            return render_template(template, variables)
    
    # Fallback to programmatic generation
    version_info = f"Version: {version}" if version else f"Updated: {date_str}"
    
    card = f"""---
license: apache-2.0
task_categories:
- genomics
- variant-annotation
tags:
- genomics
- ensembl
- vcf
- variants
- parquet
- bioinformatics
pretty_name: Ensembl Variations (Parquet)
size_categories:
- 10G<n<100G
---

# Ensembl Variations (Parquet Format)

This dataset contains Ensembl human genetic variations converted to Parquet format for fast and efficient VCF annotation.

## Dataset Description

- **Purpose**: Fast annotation of VCF files with Ensembl variation data
- **Format**: Apache Parquet (columnar storage)
- **Source**: [Ensembl Variation Database](https://www.ensembl.org/info/genome/variation/)
- **{version_info}**
- **Total Files**: {num_files}
- **Total Size**: ~{total_size_gb:.1f} GB
{variant_types_section}

## Why Parquet?

Parquet format provides significant advantages over VCF for annotation tasks:

- **10-100x faster** querying and filtering
- **Efficient compression** (smaller file sizes)
- **Column-based access** (read only what you need)
- **Native support** in modern data processing tools (Polars, DuckDB, Arrow)
- **Schema evolution** support

## Usage

### With Polars (Recommended)

```python
import polars as pl

# Load SNV variants for chromosome 21
df = pl.scan_parquet("hf://datasets/just-dna-seq/ensembl_variations/data/SNV/homo_sapiens-chr21.parquet")

# Filter variants by position
variants = df.filter(
    (pl.col("POS") >= 10000000) & (pl.col("POS") <= 20000000)
).collect()

print(variants)
```

### With DuckDB

```python
import duckdb

# Query variants directly from HuggingFace
result = duckdb.sql(\"\"\"
    SELECT * FROM 'hf://datasets/just-dna-seq/ensembl_variations/data/SNV/homo_sapiens-chr21.parquet'
    WHERE POS BETWEEN 10000000 AND 20000000
    LIMIT 10
\"\"\").df()

print(result)
```

### For VCF Annotation

```python
from genobear import VCFAnnotator

# Annotate your VCF file with Ensembl data
annotator = VCFAnnotator("your_variants.vcf")
annotated_df = annotator.annotate_with_ensembl(
    ensembl_path="hf://datasets/just-dna-seq/ensembl_variations/data/"
)
```

## Schema

Typical columns in the parquet files:

- `CHROM`: Chromosome (e.g., "chr1", "chr2", ...)
- `POS`: Position (1-based)
- `ID`: Variant ID (rsID)
- `REF`: Reference allele
- `ALT`: Alternate allele(s)
- `QUAL`: Quality score
- `FILTER`: Filter status
- `INFO_*`: Various INFO field columns
- `TSA`: Variant type annotation (SNV, deletion, indel, insertion, substitution, sequence_alteration)

## Citation

If you use this dataset, please cite:

```bibtex
@misc{{ensembl_variations_parquet,
  title = {{Ensembl Variations (Parquet Format)}},
  author = {{GenoBear Team}},
  year = {{{datetime.now().year}}},
  howpublished = {{\\url{{https://huggingface.co/datasets/just-dna-seq/ensembl_variations}}}},
  note = {{Processed from Ensembl Variation Database}}
}}
```

And the original Ensembl data:

```bibtex
@article{{martin2023ensembl,
  title={{Ensembl 2023}},
  author={{Martin, Fergal J and others}},
  journal={{Nucleic acids research}},
  volume={{51}},
  number={{D1}},
  pages={{D933--D941}},
  year={{2023}},
  publisher={{Oxford University Press}}
}}
```

## License

This dataset is released under Apache 2.0 license. The original Ensembl data is available under their terms of use.

## Maintenance

This dataset is maintained by the GenoBear project. For issues or questions:
- GitHub: [https://github.com/antonkulaga/genobear](https://github.com/antonkulaga/genobear)
- HuggingFace: [https://huggingface.co/just-dna-seq](https://huggingface.co/just-dna-seq)
"""
    
    return card


def generate_clinvar_card(
    num_files: int,
    total_size_gb: float,
    variant_types: Optional[List[str]] = None,
    version: Optional[str] = None,
    use_template: bool = True,
) -> str:
    """
    Generate a dataset card for ClinVar dataset.
    
    Args:
        num_files: Total number of parquet files in the dataset
        total_size_gb: Total size of the dataset in GB
        variant_types: List of variant types (SNV, deletion, indel, etc.)
        version: ClinVar version or date
        use_template: Whether to use template file (if available) or generate programmatically
        
    Returns:
        Markdown content for the dataset card
    """
    date_str = datetime.now().strftime("%Y-%m-%d")
    current_year = datetime.now().year
    
    # Prepare variant types section
    variant_types_section = ""
    if variant_types:
        variant_list = "\n".join(f"- `{vt}/`" for vt in sorted(variant_types))
        variant_types_section = f"""
## Dataset Structure

The dataset is organized by variant type:

{variant_list}

Each directory contains parquet files with clinical variant data.
"""
    
    # Try to load template if requested
    if use_template:
        template = load_template("clinvar_card_template.md")
        if template:
            variables = {
                "update_date": date_str,
                "num_files": str(num_files),
                "total_size_gb": f"{total_size_gb:.1f}",
                "variant_types_section": variant_types_section,
                "current_year": str(current_year),
            }
            return render_template(template, variables)
    
    # Fallback to programmatic generation
    version_info = f"Version: {version}" if version else f"Updated: {date_str}"
    
    card = f"""---
license: other
task_categories:
- genomics
- variant-annotation
- clinical-genomics
tags:
- genomics
- clinvar
- clinical-variants
- vcf
- parquet
- bioinformatics
- pathogenicity
pretty_name: ClinVar (Parquet Format)
size_categories:
- 1G<n<10G
---

# ClinVar (Parquet Format)

This dataset contains ClinVar clinical variant data converted to Parquet format for fast and efficient clinical annotation of VCF files.

## Dataset Description

- **Purpose**: Fast clinical annotation of VCF files with pathogenicity and clinical significance
- **Format**: Apache Parquet (columnar storage)
- **Source**: [ClinVar Database](https://www.ncbi.nlm.nih.gov/clinvar/)
- **{version_info}**
- **Total Files**: {num_files}
- **Total Size**: ~{total_size_gb:.1f} GB
{variant_types_section}

## Why Parquet?

Parquet format provides significant advantages over VCF for clinical annotation:

- **10-100x faster** querying for clinical significance
- **Efficient compression** (smaller file sizes)
- **Column-based access** (read only clinical fields you need)
- **Native support** in modern data processing tools (Polars, DuckDB, Arrow)
- **Easy filtering** by pathogenicity, review status, etc.

## Usage

### With Polars (Recommended)

```python
import polars as pl

# Load ClinVar data
df = pl.scan_parquet("hf://datasets/just-dna-seq/clinvar/data/*.parquet")

# Filter for pathogenic variants
pathogenic = df.filter(
    pl.col("CLNSIG").str.contains("Pathogenic")
).collect()

print(pathogenic)
```

### With DuckDB

```python
import duckdb

# Query pathogenic variants
result = duckdb.sql(\"\"\"
    SELECT CHROM, POS, REF, ALT, CLNSIG, CLNDN
    FROM 'hf://datasets/just-dna-seq/clinvar/data/*.parquet'
    WHERE CLNSIG LIKE '%Pathogenic%'
    LIMIT 100
\"\"\").df()

print(result)
```

### For VCF Clinical Annotation

```python
from genobear import VCFAnnotator

# Annotate your VCF with clinical significance
annotator = VCFAnnotator("patient_variants.vcf")
annotated_df = annotator.annotate_with_clinvar(
    clinvar_path="hf://datasets/just-dna-seq/clinvar/data/"
)

# Filter for clinically significant variants
clinical = annotated_df.filter(
    pl.col("CLNSIG").is_not_null()
)
```

## Schema

Key columns in the parquet files:

- `CHROM`: Chromosome
- `POS`: Position (GRCh38)
- `ID`: Variant ID
- `REF`: Reference allele
- `ALT`: Alternate allele
- `CLNSIG`: Clinical significance (Pathogenic, Benign, etc.)
- `CLNDN`: Disease name
- `CLNREVSTAT`: Review status
- `CLNVC`: Variant type
- `MC`: Molecular consequence
- `AF_*`: Allele frequencies from various populations

## Citation

If you use this dataset, please cite:

```bibtex
@misc{{clinvar_parquet,
  title = {{ClinVar (Parquet Format)}},
  author = {{GenoBear Team}},
  year = {{{datetime.now().year}}},
  howpublished = {{\\url{{https://huggingface.co/datasets/just-dna-seq/clinvar}}}},
  note = {{Processed from ClinVar Database}}
}}
```

And the original ClinVar database:

```bibtex
@article{{landrum2018clinvar,
  title={{ClinVar: improving access to variant interpretations and supporting evidence}},
  author={{Landrum, Melissa J and others}},
  journal={{Nucleic acids research}},
  volume={{46}},
  number={{D1}},
  pages={{D1062--D1067}},
  year={{2018}},
  publisher={{Oxford University Press}}
}}
```

## License

This dataset is processed from ClinVar public data. Please refer to [ClinVar's terms of use](https://www.ncbi.nlm.nih.gov/clinvar/docs/maintenance_use/).

## Clinical Use Disclaimer

⚠️ **Important**: This data is for research purposes only. It should not be used for clinical decision-making without proper validation and review by qualified medical professionals.

## Maintenance

This dataset is maintained by the GenoBear project. For issues or questions:
- GitHub: [https://github.com/antonkulaga/genobear](https://github.com/antonkulaga/genobear)
- HuggingFace: [https://huggingface.co/just-dna-seq](https://huggingface.co/just-dna-seq)
"""
    
    return card


def save_dataset_card(
    card_content: str,
    output_path: Path
) -> Path:
    """
    Save dataset card to a file.
    
    Args:
        card_content: Markdown content of the dataset card
        output_path: Path where to save the README.md file
        
    Returns:
        Path to the saved file
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(card_content)
    return output_path

