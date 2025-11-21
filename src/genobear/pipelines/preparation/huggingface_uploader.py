"""
Hugging Face dataset uploader pipeline.

This module provides functions to upload parquet files to Hugging Face datasets,
with intelligent file comparison to avoid unnecessary uploads.
"""

from pathlib import Path
from typing import Optional, Dict, List
from eliot import start_action
from pipefunc import pipefunc, Pipeline
from huggingface_hub import HfApi, hf_hub_download, list_repo_files, CommitOperationAdd
from huggingface_hub.utils import RepositoryNotFoundError, HfHubHTTPError
from genobear.pipelines.preparation.dataset_card_generator import (
    generate_ensembl_card, 
    generate_clinvar_card,
    save_dataset_card
)


@pipefunc(
    output_name="uploaded_files",
    renames={"parquet_file": "parquet_files", "path_in_repo": "path_in_repos"},
    mapspec="parquet_files[i], path_in_repos[i] -> uploaded_files[i]",
)
def upload_to_hf_if_changed(
    parquet_file: Path,
    repo_id: str,
    repo_type: str = "dataset",
    path_in_repo: Optional[str] = None,
    token: Optional[str] = None,
    commit_message: Optional[str] = None,
) -> Dict[str, any]:
    """
    Upload a parquet file to Hugging Face Hub only if it differs in size from the remote version.
    
    Args:
        parquet_file: Local path to the parquet file to upload
        repo_id: Hugging Face repository ID (e.g., "username/dataset-name")
        repo_type: Type of repository ("dataset", "model", or "space")
        path_in_repo: Path within the repository. If None, uses the filename
        token: Hugging Face API token. If None, uses HF_TOKEN env variable or cached token
        commit_message: Custom commit message for the upload
        
    Returns:
        Dictionary with upload information:
            - "file": local file path
            - "uploaded": whether file was uploaded
            - "reason": reason for upload or skip
            - "local_size": size of local file in bytes
            - "remote_size": size of remote file in bytes (if exists)
    """
    with start_action(
        action_type="upload_to_hf_if_changed",
        parquet_file=str(parquet_file),
        repo_id=repo_id,
        path_in_repo=path_in_repo
    ) as action:
        api = HfApi(token=token)
        
        # Determine path in repo
        if path_in_repo is None:
            path_in_repo = f"data/{parquet_file.name}"
        
        # Get local file size
        local_size = parquet_file.stat().st_size
        action.log(
            message_type="info",
            step="local_file_info",
            local_size=local_size,
            local_size_mb=round(local_size / (1024 * 1024), 2)
        )
        
        # Check if file exists on HF and get its size
        try:
            repo_files = list_repo_files(repo_id=repo_id, repo_type=repo_type, token=token)
            remote_file_exists = path_in_repo in repo_files
            
            if remote_file_exists:
                # Get remote file info to compare size
                try:
                    file_info = api.get_paths_info(
                        repo_id=repo_id,
                        paths=[path_in_repo],
                        repo_type=repo_type,
                    )
                    remote_size = file_info[0].size if file_info else None
                    
                    action.log(
                        message_type="info",
                        step="remote_file_info",
                        remote_size=remote_size,
                        remote_size_mb=round(remote_size / (1024 * 1024), 2) if remote_size else None
                    )
                    
                    # Compare sizes
                    if remote_size == local_size:
                        action.log(
                            message_type="info",
                            step="skip_upload",
                            reason="size_match"
                        )
                        return {
                            "file": str(parquet_file),
                            "uploaded": False,
                            "reason": "size_match",
                            "local_size": local_size,
                            "remote_size": remote_size,
                        }
                except Exception as e:
                    # If we can't get size info, assume we should upload
                    action.log(
                        message_type="warning",
                        step="remote_size_check_failed",
                        error=str(e)
                    )
                    remote_size = None
            else:
                remote_size = None
                action.log(
                    message_type="info",
                    step="remote_file_not_found"
                )
        except (RepositoryNotFoundError, HfHubHTTPError) as e:
            action.log(
                message_type="warning",
                step="repo_check_failed",
                error=str(e)
            )
            remote_size = None
        
        # Upload the file
        reason = "new_file" if remote_size is None else "size_differs"
        action.log(
            message_type="info",
            step="uploading",
            reason=reason
        )
        
        if commit_message is None:
            if remote_size is None:
                commit_message = f"Add {parquet_file.name}"
            else:
                commit_message = f"Update {parquet_file.name} (size changed)"
        
        try:
            api.upload_file(
                path_or_fileobj=str(parquet_file),
                path_in_repo=path_in_repo,
                repo_id=repo_id,
                repo_type=repo_type,
                commit_message=commit_message,
            )
            
            action.log(
                message_type="success",
                step="upload_complete",
                path_in_repo=path_in_repo
            )
            
            return {
                "file": str(parquet_file),
                "uploaded": True,
                "reason": reason,
                "local_size": local_size,
                "remote_size": remote_size,
            }
        except Exception as e:
            action.log(
                message_type="error",
                step="upload_failed",
                error=str(e)
            )
            raise


def collect_parquet_files(
    source_dir: Path,
    pattern: str = "**/*.parquet",
    recursive: bool = True
) -> List[Path]:
    """
    Collect parquet files from a directory, explicitly excluding VCF and other non-parquet files.
    
    Args:
        source_dir: Directory to search for parquet files
        pattern: Glob pattern for finding files (default: **/*.parquet)
        recursive: Whether to search recursively
        
    Returns:
        List of paths to parquet files (only .parquet extension files)
    """
    with start_action(
        action_type="collect_parquet_files",
        source_dir=str(source_dir),
        pattern=pattern
    ) as action:
        source_dir = Path(source_dir)
        
        if not source_dir.exists():
            action.log(
                message_type="error",
                reason="directory_not_found"
            )
            raise FileNotFoundError(f"Directory not found: {source_dir}")
        
        # Collect files matching pattern
        if recursive:
            files = list(source_dir.glob(pattern))
        else:
            files = list(source_dir.glob(pattern.replace("**/", "")))
        
        # SAFETY: Filter to ONLY parquet files, explicitly excluding VCF and other formats
        parquet_files = [f for f in files if f.suffix == ".parquet" and f.is_file()]
        
        action.log(
            message_type="info",
            total_files=len(parquet_files),
            total_size_gb=round(sum(f.stat().st_size for f in parquet_files) / (1024**3), 2)
        )
        
        return parquet_files


def upload_files_batch(
    parquet_files: List[Path],
    repo_id: str,
    path_in_repos: List[str],
    repo_type: str = "dataset",
    token: Optional[str] = None,
    commit_message: Optional[str] = None,
    dataset_card_content: Optional[str] = None,
) -> Dict[str, any]:
    """
    Upload multiple parquet files to HuggingFace in a single atomic commit.
    
    This is much more efficient than uploading files one by one, as it:
    - Creates only ONE commit for all files
    - Reduces overhead and is faster
    - Is atomic - either all files upload or none
    
    Args:
        parquet_files: List of local parquet file paths
        repo_id: Hugging Face repository ID
        path_in_repos: List of target paths in the repository (same order as parquet_files)
        repo_type: Type of repository ("dataset", "model", or "space")
        token: Hugging Face API token
        commit_message: Custom commit message
        dataset_card_content: Optional README.md content to upload as dataset card
        
    Returns:
        Dictionary with upload results for each file
    """
    with start_action(
        action_type="upload_files_batch",
        repo_id=repo_id,
        num_files=len(parquet_files)
    ) as action:
        api = HfApi(token=token)
        
        # Get list of remote files and their sizes
        action.log(message_type="info", step="fetching_remote_file_list")
        try:
            repo_files_info = {}
            repo_files = list_repo_files(repo_id=repo_id, repo_type=repo_type, token=token)
            
            # Get sizes for all remote files we care about
            paths_to_check = [p for p in path_in_repos if p in repo_files]
            if paths_to_check:
                file_infos = api.get_paths_info(
                    repo_id=repo_id,
                    paths=paths_to_check,
                    repo_type=repo_type,
                )
                repo_files_info = {info.path: info.size for info in file_infos}
                
        except (RepositoryNotFoundError, HfHubHTTPError) as e:
            action.log(
                message_type="warning",
                step="repo_check_failed",
                error=str(e)
            )
            repo_files_info = {}
        
        # Determine which files need uploading
        operations = []
        upload_results = []
        
        for parquet_file, path_in_repo in zip(parquet_files, path_in_repos):
            local_size = parquet_file.stat().st_size
            remote_size = repo_files_info.get(path_in_repo)
            
            # Check if we need to upload
            if remote_size is not None and remote_size == local_size:
                # Skip - sizes match
                upload_results.append({
                    "file": str(parquet_file),
                    "path_in_repo": path_in_repo,
                    "uploaded": False,
                    "reason": "size_match",
                    "local_size": local_size,
                    "remote_size": remote_size,
                })
            else:
                # Need to upload
                reason = "new_file" if remote_size is None else "size_differs"
                operations.append(
                    CommitOperationAdd(
                        path_or_fileobj=str(parquet_file),
                        path_in_repo=path_in_repo,
                    )
                )
                upload_results.append({
                    "file": str(parquet_file),
                    "path_in_repo": path_in_repo,
                    "uploaded": True,
                    "reason": reason,
                    "local_size": local_size,
                    "remote_size": remote_size,
                })
        
        # Add dataset card if provided
        tmp_readme_path = None
        if dataset_card_content is not None:
            import tempfile
            # Create temporary file for README
            with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as tmp:
                tmp.write(dataset_card_content)
                tmp_readme_path = tmp.name
            
            operations.append(
                CommitOperationAdd(
                    path_or_fileobj=tmp_readme_path,
                    path_in_repo="README.md",
                )
            )
            action.log(
                message_type="info",
                step="adding_dataset_card",
                card_size=len(dataset_card_content)
            )
        
        # Upload all files in a single commit
        if operations:
            num_uploading = len(operations)
            total_size_mb = sum(
                Path(op.path_or_fileobj).stat().st_size 
                for op in operations
            ) / (1024 * 1024)
            
            action.log(
                message_type="info",
                step="creating_commit",
                num_files=num_uploading,
                total_size_mb=round(total_size_mb, 2)
            )
            
            if commit_message is None:
                commit_message = f"Upload {num_uploading} parquet file{'s' if num_uploading > 1 else ''}"
            
            try:
                commit_info = api.create_commit(
                    repo_id=repo_id,
                    repo_type=repo_type,
                    operations=operations,
                    commit_message=commit_message,
                )
                
                action.log(
                    message_type="success",
                    step="commit_created",
                    commit_url=commit_info.commit_url,
                    num_files=num_uploading
                )
            except Exception as e:
                action.log(
                    message_type="error",
                    step="commit_failed",
                    error=str(e)
                )
                raise
            finally:
                # Clean up temporary README file
                if tmp_readme_path is not None:
                    import os
                    try:
                        os.unlink(tmp_readme_path)
                    except Exception:
                        pass  # Ignore cleanup errors
        else:
            action.log(
                message_type="info",
                step="no_files_to_upload",
                reason="all_sizes_match"
            )
        
        # Summary
        num_uploaded = sum(1 for r in upload_results if r["uploaded"])
        num_skipped = len(upload_results) - num_uploaded
        
        action.log(
            message_type="summary",
            total_files=len(upload_results),
            uploaded=num_uploaded,
            skipped=num_skipped
        )
        
        return {
            "uploaded_files": upload_results,
            "num_uploaded": num_uploaded,
            "num_skipped": num_skipped,
        }


def make_hf_upload_pipeline() -> Pipeline:
    """Create a pipeline for uploading parquet files to Hugging Face."""
    return Pipeline([upload_to_hf_if_changed], print_error=True)


def upload_parquet_to_hf(
    parquet_files: List[Path] | Path,
    repo_id: str,
    repo_type: str = "dataset",
    token: Optional[str] = None,
    path_prefix: str = "data",
    parallel: bool = True,
    workers: Optional[int] = None,
    source_dir: Optional[Path] = None,
    dataset_card_content: Optional[str] = None,
    **kwargs
) -> Dict[str, any]:
    """
    Upload parquet files to Hugging Face Hub, only uploading files that differ in size.
    
    Args:
        parquet_files: Single path or list of paths to parquet files
        repo_id: Hugging Face repository ID (e.g., "username/dataset-name")
        repo_type: Type of repository ("dataset", "model", or "space")
        token: Hugging Face API token. If None, uses HF_TOKEN env variable
        path_prefix: Prefix for paths in the repository (default: "data")
        parallel: Whether to upload files in parallel
        workers: Number of parallel workers
        source_dir: Source directory to compute relative paths from (preserves directory structure)
        dataset_card_content: Optional README.md content to upload as dataset card
        **kwargs: Additional arguments passed to pipeline
        
    Returns:
        Dictionary with upload results
    """
    with start_action(
        action_type="upload_parquet_to_hf",
        repo_id=repo_id,
        num_files=len(parquet_files) if isinstance(parquet_files, list) else 1
    ) as action:
        from genobear.config import get_default_workers
        
        if isinstance(parquet_files, Path):
            parquet_files = [parquet_files]
        
        # SAFETY: Ensure ONLY parquet files are in the upload list
        non_parquet = [f for f in parquet_files if f.suffix != ".parquet"]
        if non_parquet:
            action.log(
                message_type="error",
                step="non_parquet_files_detected",
                non_parquet_count=len(non_parquet),
                non_parquet_samples=str(non_parquet[:5])
            )
            raise ValueError(
                f"Only .parquet files can be uploaded. Found {len(non_parquet)} non-parquet files. "
                f"Examples: {[f.name for f in non_parquet[:3]]}"
            )
        
        # Prepare path mappings, preserving directory structure if source_dir is provided
        path_in_repos = []
        for f in parquet_files:
            if source_dir is not None:
                # Preserve directory structure relative to source_dir
                try:
                    relative_path = f.relative_to(source_dir)
                    # Use POSIX path (forward slashes) for HuggingFace
                    relative_path_str = relative_path.as_posix()
                    path_in_repos.append(f"{path_prefix}/{relative_path_str}")
                except ValueError:
                    # If file is not relative to source_dir, just use filename
                    path_in_repos.append(f"{path_prefix}/{f.name}")
            else:
                # Just use filename
                path_in_repos.append(f"{path_prefix}/{f.name}")
        
        # Log the upload plan
        action.log(
            message_type="info",
            step="preparing_upload",
            num_files=len(parquet_files),
            sample_paths=path_in_repos[:3] if len(path_in_repos) > 0 else [],
        )
        
        # Use batch upload - uploads all files in a single commit (much more efficient!)
        batch_results = upload_files_batch(
            parquet_files=parquet_files,
            repo_id=repo_id,
            path_in_repos=path_in_repos,
            repo_type=repo_type,
            token=token,
            commit_message=None,  # Auto-generated message
            dataset_card_content=dataset_card_content,
        )
        
        # Format results for compatibility with old API
        results = {
            "uploaded_files": batch_results["uploaded_files"],
            "num_uploaded": batch_results["num_uploaded"],
            "num_skipped": batch_results["num_skipped"],
        }
        
        return results

