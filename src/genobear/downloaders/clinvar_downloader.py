

from typing import Literal
from pydantic import HttpUrl, model_validator
from genobear.downloaders.vcf_downloader import VCFDownloader


class ClinVarDownloader(VCFDownloader):
    """
    A specific downloader for ClinVar VCF files, inheriting all the Pydantic
    goodness from VCFDownloader.
    """
    assembly: Literal['GRCh38', 'GRCh37'] = 'GRCh38'
    
    # Set default values for ClinVar
    base_url: HttpUrl = None  # Will be set by validator
    vcf_filename: str = "clinvar.vcf.gz"
    tbi_filename: str = "clinvar.vcf.gz.tbi"
    hash_filename: str = "clinvar.vcf.gz.md5"
    cache_subdir: str = None  # Will be set by validator
    
    @model_validator(mode='before')
    @classmethod
    def set_clinvar_defaults(cls, values):
        """Set ClinVar-specific default values based on assembly."""
        assembly = values.get('assembly', 'GRCh38')
        
        # Set base_url if not provided
        if 'base_url' not in values or values['base_url'] is None:
            values['base_url'] = f"https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_{assembly}/"
        
        # Set cache_subdir if not provided
        if 'cache_subdir' not in values or values['cache_subdir'] is None:
            values['cache_subdir'] = f"vcf_downloader/clinvar/{assembly}"
            
        return values