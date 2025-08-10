import marimo

__generated_with = "0.14.13"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo


    return


@app.cell
def _():
    import biobear as bb
    from pathlib import Path

    # Define the path to the ClinVar Parquet file using pathlib
    clinvar_file_path = Path("/home/antonkulaga/genobear/databases/clinvar/hg38/clinvar_hg38.parquet")

    # Use scan_parquet for memory-efficient lazy loading.
    # This creates a LazyFrame, which describes the operations to be performed,
    # but doesn't execute them until .collect() is called.
    lazy_clinvar_df = bb.scan_parquet(clinvar_file_path)

    # To preview the data without loading the entire file into memory,
    # we can fetch the first 10 rows.
    clinvar_preview = lazy_clinvar_df.head(10).collect()

    clinvar_preview
    return


if __name__ == "__main__":
    app.run()
