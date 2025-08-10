from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional, Set
import urllib.request

from eliot import start_action
from pydantic import HttpUrl, Field, field_validator, model_validator

from genobear.downloaders.multi_vcf_downloader import MultiVCFDownloader


class EnsemblDownloader(MultiVCFDownloader):
    """
    Downloader for Ensembl VCF files from the current_variation directory.
    
    Downloads all chromosomal VCF files from:
    https://ftp.ensembl.org/pub/current_variation/vcf/homo_sapiens/
    
    Files follow the pattern: homo_sapiens-chr{chr}.vcf.gz
    Index files follow the pattern: homo_sapiens-chr{chr}.vcf.gz.csi
    
    This downloader automatically discovers all available chromosomes or can be
    limited to specific chromosomes of interest.
    """
    
    # Base URL for Ensembl variation data
    base_url: HttpUrl = "https://ftp.ensembl.org/pub/current_variation/vcf/homo_sapiens/"
    
    # Override parent fields to provide defaults
    vcf_urls: Dict[str, str] = Field(default_factory=dict, description="Mapping of identifier (e.g. chr1) to VCF URL")
    index_urls: Optional[Dict[str, str]] = Field(default_factory=dict, description="Optional mapping of identifier to index URLs")
    known_hashes: Optional[Dict[str, str]] = Field(default=None, description="Optional mapping of identifier to known hashes")
    
    # Chromosome selection  
    chromosomes: Optional[Set[str]] = Field(
        default=None,
        description="Set of chromosomes to download. If None, downloads all available chromosomes"
    )
    
    # Whether to use CSI index files (Ensembl uses .csi instead of .tbi)
    use_csi_index: bool = Field(
        default=True,
        description="Whether to use CSI index files (.csi) instead of TBI (.tbi)"
    )
    
    # Whether to fetch and use checksums from CHECKSUMS file
    use_checksums: bool = Field(
        default=True,
        description="Whether to fetch and use checksums from the CHECKSUMS file"
    )
    
    def __init__(self, **data):
        # Set default cache subdir if not provided
        if 'cache_subdir' not in data:
            data['cache_subdir'] = 'ensembl_variation'
        
        super().__init__(**data)
    
    @field_validator('chromosomes', mode='before')
    @classmethod
    def validate_chromosomes(cls, v):
        """Validate and normalize chromosome names."""
        if v is None:
            return None
        
        if isinstance(v, (list, tuple)):
            v = set(v)
        elif isinstance(v, str):
            v = {v}
        
        # Normalize chromosome names
        normalized = set()
        for chrom in v:
            chrom_str = str(chrom)
            # Remove 'chr' prefix if present
            if chrom_str.startswith('chr'):
                chrom_str = chrom_str[3:]
            normalized.add(chrom_str)
        
        return normalized
    
    @model_validator(mode='after')
    def setup_urls_and_hashes(self):
        """Set up VCF URLs, index URLs, and known hashes after initialization."""
        with start_action(action_type="ensembl_setup", use_checksums=self.use_checksums) as action:
            # Get available chromosomes from the FTP site
            available_chromosomes = self._discover_available_chromosomes()
            action.log(message_type="info", available_chromosomes=list(available_chromosomes))
            
            # Determine which chromosomes to download
            if self.chromosomes is None:
                target_chromosomes = available_chromosomes
            else:
                target_chromosomes = self.chromosomes.intersection(available_chromosomes)
                missing = self.chromosomes - available_chromosomes
                if missing:
                    action.log(message_type="warning", missing_chromosomes=list(missing))
            
            action.log(message_type="info", target_chromosomes=list(target_chromosomes))
            
            # Build URL mappings
            vcf_urls = {}
            index_urls = {}
            
            for chrom in target_chromosomes:
                # VCF files
                vcf_filename = f"homo_sapiens-chr{chrom}.vcf.gz"
                vcf_urls[f"chr{chrom}"] = f"{self.base_url}{vcf_filename}"
                
                # Index files
                if self.use_csi_index:
                    index_filename = f"homo_sapiens-chr{chrom}.vcf.gz.csi"
                    index_urls[f"chr{chrom}"] = f"{self.base_url}{index_filename}"
            
            # Update the inherited fields
            object.__setattr__(self, 'vcf_urls', vcf_urls)
            object.__setattr__(self, 'index_urls', index_urls)
            
            # Fetch checksums if requested
            if self.use_checksums:
                with start_action(action_type="fetch_checksums") as checksum_action:
                    checksums = self._fetch_checksums()
                    if checksums:
                        object.__setattr__(self, 'known_hashes', checksums)
                        checksum_action.log(message_type="info", checksums_fetched=len(checksums))
                    else:
                        checksum_action.log(message_type="warning", checksums_unavailable=True)
                        object.__setattr__(self, 'known_hashes', None)
            
            action.log(message_type="info", setup_complete=True, num_files=len(vcf_urls))
        
        return self
    
    def _discover_available_chromosomes(self) -> Set[str]:
        """
        Discover available chromosome VCF files from the Ensembl FTP directory.
        
        Returns:
            Set of chromosome identifiers (e.g., {'1', '2', ..., '22', 'X', 'Y', 'MT'})
        """
        with start_action(action_type="discover_chromosomes") as action:
            # Try to discover from server first
            discovered = self._fetch_chromosomes_from_server()
            
            if discovered:
                action.log(message_type="info", discovered_chromosomes=list(discovered))
                return discovered
            
            # Fallback to standard human chromosomes if discovery fails
            fallback = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12',
                       '13', '14', '15', '16', '17', '18', '19', '20', '21', '22',
                       'X', 'Y', 'MT'}
            action.log(message_type="warning", using_fallback=list(fallback))
            return fallback
    
    def _fetch_chromosomes_from_server(self) -> Optional[Set[str]]:
        """
        Fetch available chromosomes from server directory listing.
        
        Returns:
            Set of chromosome identifiers if successful, None if failed
        """
        with start_action(action_type="fetch_chromosomes_from_server") as action:
            action.log(message_type="info", url=str(self.base_url))
            
            # Fetch the directory listing
            with urllib.request.urlopen(str(self.base_url)) as response:
                html_content = response.read().decode('utf-8')
            
            # Extract VCF filenames using regex
            vcf_pattern = r'homo_sapiens-chr([^.]+)\.vcf\.gz"'
            matches = re.findall(vcf_pattern, html_content)
            
            chromosomes = set(matches) if matches else None
            action.log(message_type="info", chromosomes_found=len(matches) if matches else 0)
            
            return chromosomes
    
    def _fetch_checksums(self) -> Optional[Dict[str, str]]:
        """
        Fetch checksums from the CHECKSUMS file on the FTP site.
        
        Returns:
            Dictionary mapping filenames to their checksums (md5:hash format), or None if failed
        """
        with start_action(action_type="fetch_checksums_from_server") as action:
            checksums = {}
            
            checksums_url = f"{self.base_url}CHECKSUMS"
            action.log(message_type="info", url=checksums_url)
            
            with urllib.request.urlopen(checksums_url) as response:
                content = response.read().decode('utf-8')
            
            # Parse CHECKSUMS file format (typically: hash  filename)
            for line in content.strip().split('\n'):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                parts = line.split()
                if len(parts) >= 2:
                    hash_value = parts[0]
                    filename = parts[1]
                    
                    # Only include VCF files we care about
                    if filename.startswith('homo_sapiens-chr') and filename.endswith('.vcf.gz'):
                        # Extract chromosome from filename
                        chrom_match = re.search(r'homo_sapiens-chr([^.]+)\.vcf\.gz', filename)
                        if chrom_match:
                            chrom = chrom_match.group(1)
                            identifier = f"chr{chrom}"
                            checksums[identifier] = f"md5:{hash_value}"
            
            action.log(message_type="info", checksums_parsed=len(checksums))
            return checksums if checksums else None
    
    @classmethod
    def for_chromosomes(
        cls,
        chromosomes: List[str],
        use_checksums: bool = True,
        **kwargs
    ) -> "EnsemblDownloader":
        """
        Convenience method to create a downloader for specific chromosomes.
        
        Args:
            chromosomes: List of chromosome identifiers (e.g., ['1', '2', 'X'])
            use_checksums: Whether to fetch and validate checksums
            **kwargs: Additional arguments passed to the constructor
            
        Returns:
            Configured EnsemblDownloader instance
        """
        return cls(
            chromosomes=chromosomes,
            use_checksums=use_checksums,
            **kwargs
        )
    
    @classmethod
    def for_autosomes_only(cls, **kwargs) -> "EnsemblDownloader":
        """
        Convenience method to create a downloader for autosomal chromosomes only.
        
        Returns:
            EnsemblDownloader configured for chromosomes 1-22
        """
        autosomes = [str(i) for i in range(1, 23)]
        return cls.for_chromosomes(autosomes, **kwargs)
    
    @classmethod
    def for_sex_chromosomes_only(cls, **kwargs) -> "EnsemblDownloader":
        """
        Convenience method to create a downloader for sex chromosomes only.
        
        Returns:
            EnsemblDownloader configured for chromosomes X and Y
        """
        sex_chromosomes = ['X', 'Y']
        return cls.for_chromosomes(sex_chromosomes, **kwargs)
