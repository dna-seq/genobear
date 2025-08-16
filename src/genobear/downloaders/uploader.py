from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from eliot import start_action
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from genobear.downloaders.multi_vcf_downloader import DownloadResult


class HuggingFaceUploader:
    """Uploader for HuggingFace datasets."""
    
    def __init__(
        self, 
        hf_repo: str,
        hf_token: Optional[str] = None,
        console: Optional[Console] = None
    ):
        """Initialize the HuggingFace uploader.
        
        Args:
            hf_repo: HuggingFace repository in format 'username/repo-name'
            hf_token: HuggingFace authentication token. If None, will try to get from environment
            console: Rich console for output. If None, creates a new one.
        """
        # Load environment variables
        load_dotenv()
        
        self.hf_repo = hf_repo
        # Resolve token from parameter or environment variables
        self.hf_token = hf_token or os.getenv("HF_TOKEN") or os.getenv("HUGGINGFACE_HUB_TOKEN") or os.getenv("HUGGING_FACE_HUB_TOKEN")
        self.console = console or Console()
    
    def upload_to_huggingface(
        self,
        downloaded_files: dict[str, DownloadResult]
    ) -> None:
        """Upload downloaded VCF files to HuggingFace dataset.
        
        Args:
            downloaded_files: Dictionary mapping identifiers to DownloadResult objects
        """
        
        with start_action(action_type="upload_to_huggingface") as action:
            action.log(message_type="info", hf_repo=self.hf_repo)
            try:
                from huggingface_hub import CommitOperationAdd, preupload_lfs_files, create_commit
            except ImportError:
                self.console.print("❌ huggingface_hub not available. Install with: pip install huggingface-hub", style="red")
                action.log(message_type="error", error="huggingface_hub not available")
                return
            
            # Check if token is available
            if not self.hf_token:
                self.console.print("❌ HuggingFace token required for upload. Set --hf-token or HF_TOKEN/HUGGINGFACE_HUB_TOKEN/HUGGING_FACE_HUB_TOKEN environment variable", style="red")
                return
            
            self.console.print(f"🚀 Uploading to HuggingFace: [bold blue]{self.hf_repo}[/bold blue]")
            
            repo_id = self.hf_repo
            
            # Prepare commit operations for all files
            operations = []
            skipped_files = []
            
            self.console.print("📋 Preparing upload operations...")
            for identifier, download_result in downloaded_files.items():
                if download_result.has_parquet:
                    file_path = download_result.parquet
                    file_type = "parquet"
                    
                    # Create commit operation
                    operation = CommitOperationAdd(
                        path_in_repo=f"{file_type}_files/{file_path.name}",
                        path_or_fileobj=file_path
                    )
                    operations.append(operation)
                    action.log(message_type="info", prepared_file=str(file_path), file_type=file_type)
                else:
                    skipped_files.append(identifier)
                    self.console.print(f"⚠️ Skipping {identifier}: no parquet files available", style="yellow")
            
            if not operations:
                self.console.print("❌ No files to upload", style="red")
                return
            
            # Pre-upload all files with progress tracking
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console,
                transient=True
            ) as progress:
                
                upload_task = progress.add_task(f"Pre-uploading {len(operations)} files...", total=len(operations))
                
                for i, operation in enumerate(operations):
                    file_name = Path(operation.path_in_repo).name
                    progress.update(upload_task, description=f"Pre-uploading {file_name}...")
                    
                    # Pre-upload the file
                    preupload_lfs_files(repo_id, additions=[operation], repo_type="dataset", token=self.hf_token)
                    
                    progress.advance(upload_task)
                
                progress.update(upload_task, description="✅ Pre-upload completed")
            
            # Create single commit for all files
            self.console.print("💾 Creating commit...")
            create_commit(
                repo_id=repo_id,
                operations=operations,
                commit_message=f"Upload Ensembl variation data ({len(operations)} parquet files)",
                repo_type="dataset",
                token=self.hf_token
            )
            
            self.console.print(f"✅ Successfully uploaded {len(operations)} files to HuggingFace")
            if skipped_files:
                self.console.print(f"⚠️ Skipped {len(skipped_files)} files: {', '.join(skipped_files)}", style="yellow")
            self.console.print(f"🔗 View dataset at: [link]https://huggingface.co/datasets/{repo_id}[/link]")
    
    def upload_folder(
        self,
        folder_path: Path,
        path_in_repo: Optional[str] = None,
        allow_patterns: Optional[list[str]] = None,
        ignore_patterns: Optional[list[str]] = None,
        commit_message: Optional[str] = None
    ) -> None:
        """Upload an entire folder to HuggingFace dataset using native HF upload_folder method.
        
        This preserves the complete directory structure including subfolders.
        If folder_path contains 'subfolder1', it will be uploaded as 'repo/path_in_repo/subfolder1'.
        
        Args:
            folder_path: Local path to the folder to upload (including subfolders)
            path_in_repo: Path within the HuggingFace repository where folder should be uploaded.
                         If None, uploads to repository root.
            allow_patterns: List of glob patterns to include (e.g., ["*.parquet", "*.json"])
            ignore_patterns: List of glob patterns to exclude (e.g., ["*.tmp", "**/.DS_Store"])
            commit_message: Custom commit message. If None, generates a default message.
        """
        
        with start_action(action_type="upload_folder") as action:
            action.log(message_type="info", folder_path=str(folder_path), path_in_repo=path_in_repo)
            
            # Check if HuggingFace hub is available
            try:
                from huggingface_hub import HfApi
            except ImportError:
                self.console.print("❌ huggingface_hub not available. Install with: pip install huggingface-hub", style="red")
                action.log(message_type="error", error="huggingface_hub not available")
                return
            
            # Check if token is available
            if not self.hf_token:
                self.console.print("❌ HuggingFace token required for upload. Set token or environment variables", style="red")
                return
            
            # Check if folder exists
            if not folder_path.exists() or not folder_path.is_dir():
                self.console.print(f"❌ Folder not found: {folder_path}", style="red")
                return
            
            # Count files to upload (for display purposes)
            total_files = 0
            for item in folder_path.rglob("*"):
                if item.is_file():
                    total_files += 1
            
            if total_files == 0:
                self.console.print(f"❌ No files found in {folder_path}", style="red")
                return
            
            self.console.print(f"📋 Found {total_files} files to upload from {folder_path}")
            self.console.print(f"🚀 Uploading folder to HuggingFace: [bold blue]{self.hf_repo}[/bold blue]")
            
            # Generate commit message
            default_message = f"Upload folder {folder_path.name}/ with {total_files} files"
            final_commit_message = commit_message or default_message
            
            # Use HuggingFace's native upload_folder method
            api = HfApi(token=self.hf_token)
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console,
                transient=True
            ) as progress:
                upload_task = progress.add_task("Uploading folder...", total=None)
                
                try:
                    api.upload_folder(
                        folder_path=str(folder_path),
                        repo_id=self.hf_repo,
                        path_in_repo=path_in_repo,
                        repo_type="dataset",
                        commit_message=final_commit_message,
                        allow_patterns=allow_patterns,
                        ignore_patterns=ignore_patterns
                    )
                    progress.update(upload_task, description="✅ Upload completed")
                    
                except Exception as e:
                    self.console.print(f"❌ Upload failed: {e}", style="red")
                    action.log(message_type="error", error=str(e))
                    return
            
            self.console.print(f"✅ Successfully uploaded folder {folder_path.name}/ to HuggingFace")
            if path_in_repo:
                self.console.print(f"📁 Uploaded to: {path_in_repo}/")
            else:
                self.console.print("📁 Uploaded to repository root")
            self.console.print(f"🔗 View dataset at: [link]https://huggingface.co/datasets/{self.hf_repo}[/link]")
