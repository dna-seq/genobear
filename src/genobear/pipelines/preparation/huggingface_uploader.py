"""
Hugging Face dataset uploader pipeline.

This module provides functions to upload parquet files to Hugging Face datasets,
with intelligent file comparison to avoid unnecessary uploads.
"""

from pathlib import Path
from typing import Optional, Dict, List
from eliot import start_action
from pipefunc import pipefunc, Pipeline
from huggingface_hub import HfApi, hf_hub_download, list_repo_files
from huggingface_hub.utils import RepositoryNotFoundError, HfHubHTTPError


@pipefunc(
    output_name="uploaded_files",
    renames={"parquet_file": "parquet_files"},
    mapspec="parquet_files[i] -> uploaded_files[i]",
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
    Collect parquet files from a directory.
    
    Args:
        source_dir: Directory to search for parquet files
        pattern: Glob pattern for finding files
        recursive: Whether to search recursively
        
    Returns:
        List of paths to parquet files
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
        
        # Collect files
        if recursive:
            files = list(source_dir.glob(pattern))
        else:
            files = list(source_dir.glob(pattern.replace("**/", "")))
        
        # Filter to only parquet files
        parquet_files = [f for f in files if f.suffix == ".parquet" and f.is_file()]
        
        action.log(
            message_type="info",
            total_files=len(parquet_files),
            total_size_gb=round(sum(f.stat().st_size for f in parquet_files) / (1024**3), 2)
        )
        
        return parquet_files


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
        
        if workers is None:
            workers = get_default_workers()
        
        pipeline = make_hf_upload_pipeline()
        
        # Prepare path mappings
        path_in_repos = [f"{path_prefix}/{f.name}" for f in parquet_files]
        
        inputs = {
            "parquet_files": parquet_files,  # This will be renamed to parquet_file by pipefunc
            "repo_id": [repo_id] * len(parquet_files),
            "repo_type": [repo_type] * len(parquet_files),
            "path_in_repo": path_in_repos,
            "token": [token] * len(parquet_files),
            **kwargs
        }
        
        results = pipeline.map(
            inputs=inputs,
            output_names={"uploaded_files"},
            parallel=parallel and (workers > 1),
            return_results=True,
        )
        
        # Summarize results
        uploaded_files = results.get("uploaded_files", [])
        if hasattr(uploaded_files, "output"):
            uploaded_files = uploaded_files.output
        if hasattr(uploaded_files, "ravel"):
            uploaded_files = uploaded_files.ravel().tolist()
        
        num_uploaded = sum(1 for r in uploaded_files if r.get("uploaded", False))
        num_skipped = len(uploaded_files) - num_uploaded
        
        action.log(
            message_type="summary",
            total_files=len(uploaded_files),
            uploaded=num_uploaded,
            skipped=num_skipped
        )
        
        return results

