"""
Database discovery utilities for GenoBear.
"""

import glob
from pathlib import Path
from typing import Dict, List, Optional
from eliot import start_action

from genobear.config import DATABASE_FOLDERS, get_database_folder


def discover_annotation_databases(
    databases_folder: Optional[Path] = None,
    database_type: Optional[str] = None,
    assembly: Optional[str] = None
) -> Dict[str, List[Path]]:
    """
    Discover available annotation databases using standardized paths.
    
    Args:
        databases_folder: Path to the databases folder (uses config default if None)
        database_type: Specific database type to look for (optional)
        assembly: Specific assembly to filter by (optional)
        
    Returns:
        Dictionary mapping database types to lists of available parquet files
    """
    if databases_folder is None:
        databases_folder = Path(next(iter(DATABASE_FOLDERS.values()))).parent
    
    with start_action(action_type="discover_databases", 
                     databases_folder=str(databases_folder),
                     database_type=database_type,
                     assembly=assembly) as action:
        
        discovered = {}
        
        if not databases_folder.exists():
            action.log("Databases folder does not exist", path=str(databases_folder))
            return discovered
        
        # Search patterns for different database types using our standardized structure
        databases_to_search = [database_type] if database_type else DATABASE_FOLDERS.keys()
        
        for db_type in databases_to_search:
            discovered[db_type] = []
            db_folder = get_database_folder(db_type)
            
            if not db_folder.exists():
                continue
            
            # Search for parquet files in the database folder
            # Pattern: db_folder/assembly/[release/]*.parquet
            search_patterns = [
                db_folder / "*" / "*.parquet",  # db_folder/assembly/*.parquet
                db_folder / "*" / "*" / "*.parquet",  # db_folder/assembly/release/*.parquet
            ]
            
            for pattern in search_patterns:
                matches = glob.glob(str(pattern))
                
                for match in matches:
                    path = Path(match)
                    # Filter by assembly if specified
                    if assembly and assembly not in str(path):
                        continue
                    discovered[db_type].append(path)
        
        # Remove duplicates and empty entries
        for db_type in discovered:
            discovered[db_type] = list(set(discovered[db_type]))
        discovered = {k: v for k, v in discovered.items() if v}
        
        action.add_success_fields(
            discovered_databases=list(discovered.keys()),
            total_files=sum(len(files) for files in discovered.values())
        )
        
        return discovered


def get_available_assemblies(databases_folder: Optional[Path] = None) -> List[str]:
    """
    Get list of available genome assemblies from database folders.
    
    Args:
        databases_folder: Path to databases folder
        
    Returns:
        List of available assembly names
    """
    if databases_folder is None:
        databases_folder = Path(next(iter(DATABASE_FOLDERS.values()))).parent
    
    assemblies = set()
    
    for db_folder in DATABASE_FOLDERS.values():
        if db_folder.exists():
            for assembly_dir in db_folder.iterdir():
                if assembly_dir.is_dir() and assembly_dir.name in ['hg19', 'hg38', 'mm9', 'mm10']:
                    assemblies.add(assembly_dir.name)
    
    return sorted(list(assemblies))


def get_available_releases(database_type: str, assembly: str) -> List[str]:
    """
    Get available releases for a specific database and assembly.
    
    Args:
        database_type: Type of database
        assembly: Genome assembly
        
    Returns:
        List of available releases
    """
    db_folder = get_database_folder(database_type)
    assembly_folder = db_folder / assembly
    
    if not assembly_folder.exists():
        return []
    
    releases = []
    for item in assembly_folder.iterdir():
        if item.is_dir() and item.name not in ['default', 'latest']:
            releases.append(item.name)
    
    return sorted(releases, reverse=True)  # Most recent first