"""
Download utilities for GenoBear.
"""

import aiohttp
import aiofiles
import os
import time
import asyncio
from pathlib import Path
from typing import Optional
import typer
from eliot import start_action


def _is_ci_environment() -> bool:
    """Check if we're running in a CI environment."""
    return any(var in os.environ for var in [
        'GENOBEAR_CI_MODE', 'CI', 'GITHUB_ACTIONS', 'GITLAB_CI', 'TRAVIS', 'CIRCLECI'
    ])


def _get_download_timeout() -> int:
    """Get download timeout from environment or default."""
    return int(os.getenv('GENOBEAR_DOWNLOAD_TIMEOUT', '600'))  # 10 minutes default


async def download_file_async(
    session: aiohttp.ClientSession,
    url: str,
    dest_path: Path,
    chunk_size: int = 8192
) -> Optional[Path]:
    """
    Download a file asynchronously with progress tracking and CI-friendly timeout handling.
    
    Args:
        session: aiohttp ClientSession for making requests
        url: URL to download from
        dest_path: Destination path for the downloaded file
        chunk_size: Size of chunks to download at a time
        
    Returns:
        Path to downloaded file if successful, None if failed
    """
    is_ci = _is_ci_environment()
    timeout_seconds = _get_download_timeout()
    
    with start_action(action_type="download_file", url=url, dest_path=str(dest_path), is_ci=is_ci) as action:
        try:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Set up timeout for CI environments
            timeout = aiohttp.ClientTimeout(total=timeout_seconds) if is_ci else None
            
            async with session.get(url, timeout=timeout) as response:
                if response.status == 200:
                    total_size = int(response.headers.get('content-length', 0))
                    downloaded = 0
                    last_progress_time = time.time()
                    progress_interval = 30 if is_ci else 5  # Log every 30s in CI, 5s locally
                    
                    action.log("Starting download", 
                              total_size=total_size, 
                              timeout_seconds=timeout_seconds,
                              is_ci=is_ci)
                    
                    if is_ci:
                        print(f"📥 Starting download: {dest_path.name} ({total_size:,} bytes)")
                    
                    async with aiofiles.open(dest_path, 'wb') as file:
                        async for chunk in response.content.iter_chunked(chunk_size):
                            await file.write(chunk)
                            downloaded += len(chunk)
                            
                            # Progress reporting with CI-specific handling
                            current_time = time.time()
                            if current_time - last_progress_time >= progress_interval or downloaded == total_size:
                                if total_size > 0:
                                    progress = (downloaded / total_size) * 100
                                    if is_ci:
                                        # CI-friendly progress logging to prevent timeouts
                                        print(f"⏳ Download progress: {dest_path.name} - {progress:.1f}% ({downloaded:,}/{total_size:,} bytes)")
                                        action.log("Download progress", 
                                                  progress_percent=progress,
                                                  bytes_downloaded=downloaded,
                                                  total_bytes=total_size)
                                    else:
                                        typer.echo(f"\rDownloading {dest_path.name}: {progress:.1f}%", nl=False)
                                else:
                                    if is_ci:
                                        print(f"⏳ Downloaded {downloaded:,} bytes of {dest_path.name}")
                                
                                last_progress_time = current_time
                    
                    if is_ci:
                        print(f"✅ Download completed: {dest_path.name} ({downloaded:,} bytes)")
                    else:
                        typer.echo(f"\n✅ Downloaded: {dest_path.name}")
                    
                    action.add_success_fields(downloaded_bytes=downloaded, success=True)
                    return dest_path
                else:
                    error_msg = f"HTTP {response.status} error downloading {url}"
                    action.log("HTTP error", status_code=response.status, url=url)
                    if is_ci:
                        print(f"❌ {error_msg}")
                    else:
                        typer.echo(f"❌ Failed to download {url}: HTTP {response.status}")
                    return None
                    
        except asyncio.TimeoutError:
            error_msg = f"Download timeout ({timeout_seconds}s) for {url}"
            action.log("Download timeout", timeout_seconds=timeout_seconds, url=url)
            if is_ci:
                print(f"⏰ {error_msg}")
            else:
                typer.echo(f"⏰ {error_msg}")
            return None
        except Exception as e:
            action.log("Download failed with exception", error=str(e), url=url)
            if is_ci:
                print(f"❌ Error downloading {url}: {e}")
            else:
                typer.echo(f"❌ Error downloading {url}: {e}")
            return None


def create_default_symlink(source: Path, link_name: str) -> Optional[Path]:
    """Create a default symlink for easy access."""
    if not source.exists():
        return None
    
    link_path = source.parent / link_name
    
    try:
        # Remove existing symlink if it exists
        if link_path.is_symlink():
            link_path.unlink()
        elif link_path.exists():
            # Don't overwrite real files
            return None
        
        # Create new symlink
        link_path.symlink_to(source.name)
        return link_path
    except Exception:
        return None