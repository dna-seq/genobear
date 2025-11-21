---
license: other
task_categories:
- other
tags:
- biology
- genomics
- clinvar
- clinical-variants
- vcf
- parquet
- bioinformatics
- pathogenicity
- variant-annotation
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
- **Updated**: {{update_date}}
- **Total Files**: {{num_files}}
- **Total Size**: ~{{total_size_gb}} GB

{{variant_types_section}}

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
result = duckdb.sql("""
    SELECT CHROM, POS, REF, ALT, CLNSIG, CLNDN
    FROM 'hf://datasets/just-dna-seq/clinvar/data/*.parquet'
    WHERE CLNSIG LIKE '%Pathogenic%'
    LIMIT 100
""").df()

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
@misc{clinvar_parquet,
  title = {ClinVar (Parquet Format)},
  author = {GenoBear Team},
  year = {{{current_year}}},
  howpublished = {\url{https://huggingface.co/datasets/just-dna-seq/clinvar}},
  note = {Processed from ClinVar Database}
}
```

And the original ClinVar database:

```bibtex
@article{landrum2018clinvar,
  title={ClinVar: improving access to variant interpretations and supporting evidence},
  author={Landrum, Melissa J and others},
  journal={Nucleic acids research},
  volume={46},
  number={D1},
  pages={D1062--D1067},
  year={2018},
  publisher={Oxford University Press}
}
```

## License

This dataset is processed from ClinVar public data. Please refer to [ClinVar's terms of use](https://www.ncbi.nlm.nih.gov/clinvar/docs/maintenance_use/).

## Clinical Use Disclaimer

⚠️ **Important**: This data is for research purposes only. It should not be used for clinical decision-making without proper validation and review by qualified medical professionals.

## Maintenance

This dataset is maintained by the GenoBear project. For issues or questions:
- GitHub: [https://github.com/antonkulaga/genobear](https://github.com/antonkulaga/genobear)
- HuggingFace: [https://huggingface.co/just-dna-seq](https://huggingface.co/just-dna-seq)

