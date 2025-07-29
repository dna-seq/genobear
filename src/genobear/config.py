"""
Configuration module for GenoBear - unified constants and defaults.
"""
import os
from pathlib import Path
from typing import Dict

# Base directories
DEFAULT_BASE_FOLDER = Path(os.path.expanduser(os.getenv("GENOBEAR_BASE_FOLDER", "~/genobear")))
DEFAULT_DATABASES_FOLDER = DEFAULT_BASE_FOLDER / "databases"
DEFAULT_OUTPUT_FOLDER = DEFAULT_BASE_FOLDER / "annotations"
DEFAULT_LOGS_FOLDER = DEFAULT_BASE_FOLDER / "logs"

# Database-specific folders
DEFAULT_DBSNP_FOLDER = DEFAULT_DATABASES_FOLDER / "dbsnp"
DEFAULT_CLINVAR_FOLDER = DEFAULT_DATABASES_FOLDER / "clinvar"
DEFAULT_ANNOVAR_FOLDER = DEFAULT_DATABASES_FOLDER / "annovar"
DEFAULT_HGMD_FOLDER = DEFAULT_DATABASES_FOLDER / "hgmd"
DEFAULT_REFSEQ_FOLDER = DEFAULT_DATABASES_FOLDER / "refseq"
DEFAULT_EXOMISER_FOLDER = DEFAULT_DATABASES_FOLDER / "exomiser"
DEFAULT_DBNSFP_FOLDER = DEFAULT_DATABASES_FOLDER / "dbnsfp"

# Database URLs
DEFAULT_DBSNP_URL = os.getenv("GENOBEAR_DBSNP_URL", "https://ftp.ncbi.nih.gov/snp/archive")
DEFAULT_CLINVAR_URL = os.getenv("GENOBEAR_CLINVAR_URL", "https://ftp.ncbi.nlm.nih.gov/pub/clinvar")
DEFAULT_ANNOVAR_URL = os.getenv("GENOBEAR_ANNOVAR_URL", "http://www.openbioinformatics.org/annovar/download")
DEFAULT_REFSEQ_URL = os.getenv("GENOBEAR_REFSEQ_URL", "https://hgdownload.soe.ucsc.edu/goldenPath")
DEFAULT_EXOMISER_URL = os.getenv("GENOBEAR_EXOMISER_URL", "https://data.monarchinitiative.org/exomiser")

# Assembly mappings
DEFAULT_ASSEMBLIES_MAP = {"hg19": "25", "hg38": "40"}
DEFAULT_CLINVAR_ASSEMBLIES_MAP = {"hg19": "vcf_GRCh37", "hg38": "vcf_GRCh38"}

# Default settings
DEFAULT_URL_PREFIX = os.getenv("GENOBEAR_DBSNP_URL_PREFIX", "GCF_000001405")
DEFAULT_ASSEMBLY = os.getenv("GENOBEAR_DEFAULT_ASSEMBLY", "hg38")
DEFAULT_RELEASE = os.getenv("GENOBEAR_DEFAULT_DBSNP_RELEASE", "b156")
DEFAULT_MAX_CONCURRENT = int(os.getenv("GENOBEAR_MAX_CONCURRENT_DOWNLOADS", "3"))
DEFAULT_PARQUET_BATCH_SIZE = int(os.getenv("GENOBEAR_PARQUET_BATCH_SIZE", "100000"))

# CI and timeout configuration
CI_DOWNLOAD_TIMEOUT = int(os.getenv("GENOBEAR_DOWNLOAD_TIMEOUT", "600"))  # 10 minutes default
CI_MAX_CONCURRENT = int(os.getenv("GENOBEAR_MAX_CONCURRENT_DOWNLOADS", "3"))
CI_PROGRESS_INTERVAL = int(os.getenv("GENOBEAR_CI_PROGRESS_INTERVAL", "30"))  # seconds
IS_CI_ENVIRONMENT = any(var in os.environ for var in [
    'GENOBEAR_CI_MODE', 'CI', 'GITHUB_ACTIONS', 'GITLAB_CI', 'TRAVIS', 'CIRCLECI'
])

# Enhanced logging for CI environments
if IS_CI_ENVIRONMENT:
    # Use print for immediate output in CI
    DEFAULT_LOG_LEVEL = "INFO"
    CI_FRIENDLY_LOGGING = True
else:
    DEFAULT_LOG_LEVEL = "WARNING"
    CI_FRIENDLY_LOGGING = False

# Database type mappings
DATABASE_FOLDERS = {
    "dbsnp": DEFAULT_DBSNP_FOLDER,
    "clinvar": DEFAULT_CLINVAR_FOLDER,
    "annovar": DEFAULT_ANNOVAR_FOLDER,
    "hgmd": DEFAULT_HGMD_FOLDER,
    "refseq": DEFAULT_REFSEQ_FOLDER,
    "exomiser": DEFAULT_EXOMISER_FOLDER,
    "dbnsfp": DEFAULT_DBNSFP_FOLDER,
}

DATABASE_URLS = {
    "dbsnp": DEFAULT_DBSNP_URL,
    "clinvar": DEFAULT_CLINVAR_URL,
    "annovar": DEFAULT_ANNOVAR_URL,
    "refseq": DEFAULT_REFSEQ_URL,
    "exomiser": DEFAULT_EXOMISER_URL,
}

# Supported database types with descriptions
SUPPORTED_DATABASES = {
    "dbsnp": "dbSNP - Single Nucleotide Polymorphism Database",
    "clinvar": "ClinVar - Clinical Variation Database", 
    "dbnsfp": "dbNSFP - Database for Non-synonymous SNPs",
    "hgmd": "HGMD - Human Gene Mutation Database",
    "annovar": "ANNOVAR - Functional annotation databases",
    "refseq": "RefSeq - Reference Sequence Database",
    "exomiser": "Exomiser - Variant prioritization tool"
}

def get_database_folder(database_type: str) -> Path:
    """Get the folder path for a specific database type."""
    return DATABASE_FOLDERS.get(database_type, DEFAULT_DATABASES_FOLDER / database_type)

def get_database_url(database_type: str) -> str:
    """Get the base URL for a specific database type."""
    return DATABASE_URLS.get(database_type, "")

def get_parquet_path(database_type: str, assembly: str, release: str = None) -> Path:
    """
    Get standardized parquet file path for a database.
    
    Args:
        database_type: Type of database (dbsnp, clinvar, etc.)
        assembly: Genome assembly (hg19, hg38)
        release: Database release/version (optional)
    
    Returns:
        Path to parquet file
    """
    base_folder = get_database_folder(database_type)
    
    if release:
        # For versioned databases like dbSNP
        return base_folder / assembly / release / f"{database_type}_{assembly}_{release}.parquet"
    else:
        # For unversioned databases or latest versions
        return base_folder / assembly / f"{database_type}_{assembly}.parquet"

def get_vcf_path(database_type: str, assembly: str, release: str = None) -> Path:
    """
    Get standardized VCF file path for a database.
    
    Args:
        database_type: Type of database (dbsnp, clinvar, etc.)
        assembly: Genome assembly (hg19, hg38)
        release: Database release/version (optional)
    
    Returns:
        Path to VCF file
    """
    base_folder = get_database_folder(database_type)
    
    if release:
        # For versioned databases like dbSNP
        return base_folder / assembly / release
    else:
        # For unversioned databases or latest versions
        return base_folder / assembly / "latest"