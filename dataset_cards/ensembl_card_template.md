---
license: apache-2.0
task_categories:
- other
tags:
- biology
- genomics
- ensembl
- vcf
- variants
- parquet
- bioinformatics
- variant-annotation
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
- **Updated**: {{update_date}}
- **Total Files**: {{num_files}}
- **Total Size**: ~{{total_size_gb}} GB

{{variant_types_section}}

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
result = duckdb.sql("""
    SELECT * FROM 'hf://datasets/just-dna-seq/ensembl_variations/data/SNV/homo_sapiens-chr21.parquet'
    WHERE POS BETWEEN 10000000 AND 20000000
    LIMIT 10
""").df()

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
@misc{ensembl_variations_parquet,
  title = {Ensembl Variations (Parquet Format)},
  author = {GenoBear Team},
  year = {{{current_year}}},
  howpublished = {\url{https://huggingface.co/datasets/just-dna-seq/ensembl_variations}},
  note = {Processed from Ensembl Variation Database}
}
```

And the original Ensembl data:

```bibtex
@article{martin2023ensembl,
  title={Ensembl 2023},
  author={Martin, Fergal J and others},
  journal={Nucleic acids research},
  volume={51},
  number={D1},
  pages={D933--D941},
  year={2023},
  publisher={Oxford University Press}
}
```

## License

This dataset is released under Apache 2.0 license. The original Ensembl data is available under their terms of use.

## Maintenance

This dataset is maintained by the GenoBear project. For issues or questions:
- GitHub: [https://github.com/antonkulaga/genobear](https://github.com/antonkulaga/genobear)
- HuggingFace: [https://huggingface.co/just-dna-seq](https://huggingface.co/just-dna-seq)

