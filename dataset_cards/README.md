# Dataset Card Templates

This directory contains template files for generating HuggingFace dataset cards (README.md) for genomic datasets.

## How It Works

When you run `uv run prepare upload-ensembl` or `uv run prepare upload-clinvar`, the uploader will:

1. **Check for template file** in this directory
2. **Replace template variables** like `{{num_files}}`, `{{total_size_gb}}`, etc. with actual values
3. **Upload the rendered card** as README.md to HuggingFace

## Available Templates

- `ensembl_card_template.md` - For Ensembl variations dataset
- `clinvar_card_template.md` - For ClinVar dataset

## Template Variables

You can use the following placeholders in your templates:

| Variable | Description | Example |
|----------|-------------|---------|
| `{{update_date}}` | Current date | `2025-11-21` |
| `{{num_files}}` | Total number of parquet files | `176` |
| `{{total_size_gb}}` | Total dataset size in GB | `28.5` |
| `{{variant_types_section}}` | Auto-generated section listing variant types | (rendered markdown) |
| `{{current_year}}` | Current year | `2025` |

## Customization

To customize the dataset card:

1. **Edit the template file** directly (e.g., `ensembl_card_template.md`)
2. **Keep the template variables** intact (e.g., `{{num_files}}`)
3. **Add/modify** any other content as needed
4. **Save** the file
5. **Run the upload** command - it will use your customized template

## Example

In `ensembl_card_template.md`:

```markdown
## Dataset Description

- **Total Files**: {{num_files}}
- **Total Size**: ~{{total_size_gb}} GB
- **Updated**: {{update_date}}
```

Will be rendered as:

```markdown
## Dataset Description

- **Total Files**: 176
- **Total Size**: ~28.5 GB
- **Updated**: 2025-11-21
```

## Version Control

These templates are version controlled in git, so you can:
- Track changes to dataset descriptions
- Review modifications before upload
- Maintain consistency across uploads

## Fallback

If a template file is not found, the system will fall back to programmatic generation of the dataset card.

