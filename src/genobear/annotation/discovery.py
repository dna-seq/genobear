"""
Database discovery functions for annotation.
"""

from pathlib import Path
from typing import Dict, List, Optional

from genobear.utils.discovery import (
    discover_annotation_databases as _discover_annotation_databases,
    get_available_assemblies as _get_available_assemblies,
    get_available_releases as _get_available_releases
)


def discover_databases(
    databases_folder: Optional[Path] = None,
    database_type: Optional[str] = None,
    assembly: Optional[str] = None
) -> Dict[str, List[Path]]:
    """
    Discover available annotation databases.
    
    Args:
        databases_folder: Path to databases folder
        database_type: Specific database type to look for
        assembly: Specific assembly to filter by
        
    Returns:
        Dictionary mapping database types to lists of available files
    """
    return _discover_annotation_databases(databases_folder, database_type, assembly)


def get_available_assemblies(databases_folder: Optional[Path] = None) -> List[str]:
    """
    Get list of available genome assemblies.
    
    Args:
        databases_folder: Path to databases folder
        
    Returns:
        List of available assembly names
    """
    return _get_available_assemblies(databases_folder)


def get_available_releases(database_type: str, assembly: str) -> List[str]:
    """
    Get available releases for a database and assembly.
    
    Args:
        database_type: Type of database
        assembly: Genome assembly
        
    Returns:
        List of available releases
    """
    return _get_available_releases(database_type, assembly)