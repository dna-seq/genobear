

from typing import Literal, Dict
from pydantic import HttpUrl, model_validator
from genobear.downloaders.multi_vcf_downloader import MultiVCFDownloader

#df.select(pl.col("tsa")).unique(())

class ClinVarDownloader(MultiVCFDownloader):
    """
    A specific downloader for ClinVar VCF files, inheriting from MultiVCFDownloader
    to leverage parquet conversion, variant splitting, and modern data processing capabilities.
    """
    assembly: Literal['GRCh38', 'GRCh37'] = 'GRCh38'
    
    # Set default values for ClinVar - these will be converted to vcf_urls/index_urls
    base_url: HttpUrl = None  # Will be set by validator
    vcf_filename: str = "clinvar.vcf.gz"
    tbi_filename: str = "clinvar.vcf.gz.tbi"
    hash_filename: str = "clinvar.vcf.gz.md5"
    
    @model_validator(mode='before')
    @classmethod
    def set_clinvar_defaults(cls, values):
        """Set ClinVar-specific default values based on assembly and convert to MultiVCFDownloader format."""
        assembly = values.get('assembly', 'GRCh38')
        
        # Set base_url if not provided
        if 'base_url' not in values or values['base_url'] is None:
            values['base_url'] = f"https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_{assembly}/"
        
        # Set subdir_name for MultiVCFDownloader (replaces cache_subdir)
        if 'subdir_name' not in values or values['subdir_name'] is None:
            values['subdir_name'] = f"clinvar/{assembly}"
        
        # Convert single VCF to MultiVCFDownloader format
        base_url = str(values['base_url'])
        vcf_filename = values.get('vcf_filename', 'clinvar.vcf.gz')
        tbi_filename = values.get('tbi_filename', 'clinvar.vcf.gz.tbi')
        hash_filename = values.get('hash_filename', 'clinvar.vcf.gz.md5')
        
        # Create URLs for MultiVCFDownloader
        vcf_url = f"{base_url}{vcf_filename}"
        index_url = f"{base_url}{tbi_filename}"
        
        # Set vcf_urls and index_urls expected by MultiVCFDownloader
        values['vcf_urls'] = {'clinvar': vcf_url}
        values['index_urls'] = {'clinvar': index_url}
        
        # Set known_hashes if hash_filename is provided
        if hash_filename:
            # We'll need to fetch the hash later, for now set up the structure
            # The hash will be fetched by MultiVCFDownloader if needed
            pass
            
        return values