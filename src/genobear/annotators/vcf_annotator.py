from pathlib import Path
from typing import Any
import polars as pl
from eliot import start_action

from genobear.annotators.base_annotator import Annotator
from genobear.io import read_vcf_file


class VCFAnnotator(Annotator[Path, pl.LazyFrame]):
    """
    An annotator that reads a VCF file from a Path and returns a Polars LazyFrame.
    
    This annotator transforms a file path into genomic variant data using polars-bio,
    enabling it to be used as the first step in annotation pipelines.
    """
    
    def __init__(self, name: str = "vcf_reader", info_fields: list[str] = None, 
                 streaming: bool = True, save_parquet: str = "auto", 
                 *dependencies: 'Annotator[Any, Path]') -> None:
        """
        Initialize the VCF annotator.
        
        Args:
            name: Unique name for this annotator instance
            info_fields: INFO fields to read from VCF. Defaults to empty list for faster reading
            streaming: Whether to use streaming mode. Defaults to True (same as read_vcf_file)
            save_parquet: Controls parquet saving. Defaults to "auto" (saves next to VCF)
            *dependencies: Optional dependencies that produce Path outputs
        """
        super().__init__(name, *dependencies)
        self.info_fields = info_fields if info_fields is not None else []
        self.streaming = streaming
        self.save_parquet = save_parquet

    def annotate(self, data: Path, **kwargs: Any) -> pl.LazyFrame:
        """
        Read a VCF file and return a Polars LazyFrame.
        
        Args:
            data: Path to the VCF file (.vcf or .vcf.gz)
            **kwargs: Additional arguments passed to read_vcf_file
            
        Returns:
            Polars LazyFrame containing the VCF data
        """
        with start_action(action_type="vcf_annotator_read", vcf_path=str(data)) as action:
            # Use read_vcf_file defaults but allow kwargs to override
            local_kwargs = dict(kwargs)
            local_kwargs.setdefault("save_parquet", self.save_parquet)
            local_kwargs.setdefault("info_fields", self.info_fields)
            local_kwargs.setdefault("streaming", self.streaming)
            
            action.log(message_type="info", reading_vcf=True, path=str(data), 
                      info_fields=self.info_fields, streaming=self.streaming,
                      save_parquet=self.save_parquet)
            
            result = read_vcf_file(file_path=data, **local_kwargs)
            
            # Ensure we always return a LazyFrame
            if isinstance(result, pl.DataFrame):
                action.log(message_type="info", converted_from_df=True)
                return result.lazy()
            
            action.log(message_type="info", vcf_read_complete=True, 
                      result_type=type(result).__name__)
            return result


if __name__ == "__main__":
    import biobear as bb
    from biobear.reader import Reader
    sessions = bb.new_session()
    sessions.read_vcf_file("/home/antonkulaga/.cache/ensembl_variation/homo_sapiens-chr21.vcf")
    #chr21.head()