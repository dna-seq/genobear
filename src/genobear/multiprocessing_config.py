"""
Multiprocessing configuration to avoid Polars fork() warnings.

This module should be imported early in the application lifecycle
to configure multiprocessing to use 'spawn' instead of 'fork' on Linux.
"""
import multiprocessing
import os
import sys
from typing import Optional


def configure_multiprocessing_context() -> Optional[str]:
    """
    Configure multiprocessing to use 'spawn' context instead of 'fork' on Linux.
    
    This prevents Polars deadlock warnings when using ThreadPoolExecutor or 
    other multiprocessing features.
    
    Returns:
        The multiprocessing start method that was set, or None if not changed.
    """
    # Only configure on Linux/Unix systems where fork() is the default
    if sys.platform.startswith('linux') or sys.platform.startswith('unix'):
        try:
            # Set multiprocessing start method to 'spawn'
            multiprocessing.set_start_method('spawn', force=True)
            return 'spawn'
        except RuntimeError:
            # Start method was already set, which is fine
            current_method = multiprocessing.get_start_method()
            if current_method == 'fork':
                # If we can't change it and it's still fork, warn the user
                import warnings
                warnings.warn(
                    "Multiprocessing is using 'fork' method which may cause "
                    "Polars deadlocks. Consider setting POLARS_ALLOW_FORKING_THREAD=1 "
                    "environment variable.",
                    RuntimeWarning
                )
            return current_method
    
    return None


def suppress_polars_fork_warning() -> None:
    """
    Set environment variable to suppress Polars fork() warnings.
    
    This is a fallback option when multiprocessing context cannot be changed.
    """
    os.environ["POLARS_ALLOW_FORKING_THREAD"] = "1"


# Auto-configure when module is imported
def _auto_configure():
    """Automatically configure multiprocessing when module is imported."""
    # First try to set spawn context
    result = configure_multiprocessing_context()
    
    # If that didn't work or we're still on fork, suppress the warning
    if result == 'fork' or result is None:
        suppress_polars_fork_warning()


# Execute auto-configuration
_auto_configure()


