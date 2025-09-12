#!/usr/bin/env python3
"""
Pytest configuration and shared fixtures for genobear tests.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
import pooch
from pycomfort.logging import to_nice_stdout


def pytest_addoption(parser):
    """Add CLI flags.

    By default we keep and reuse the shared pooch cache across test runs.
    Pass --no-shared-pooch-cache to force temporary, cleaned caches.
    """
    parser.addoption(
        "--no-shared-pooch-cache",
        action="store_true",
        default=False,
        help=(
            "Use temporary per-test pooch caches and clean them after tests. "
            "Defaults to False (shared pooch cache is reused)."
        ),
    )
    parser.addoption(
        "--clean-cache",
        action="store_true",
        default=False,
        help="Clean the cache directory before running tests"
    )


@pytest.fixture(scope="session")
def use_shared_pooch_cache(request) -> bool:
    """Whether to use the shared pooch cache across tests (default True)."""
    return not request.config.getoption("--no-shared-pooch-cache")


@pytest.fixture(scope="session")
def shared_pooch_cache_dir() -> Path:
    """Path to the shared pooch cache used by downloaders by default."""
    return Path(pooch.os_cache("ensembl_variation"))


@pytest.fixture(scope="session")
def test_data_dir():
    """Create a temporary directory for test data that persists across tests in a session."""
    temp_dir = tempfile.mkdtemp(prefix="genobear_test_data_")
    yield Path(temp_dir)
    # Cleanup after all tests
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for a single test."""
    temp_dir = tempfile.mkdtemp(prefix="genobear_test_")
    yield Path(temp_dir)
    # Cleanup after test
    shutil.rmtree(temp_dir, ignore_errors=True)


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (may be slow)"
    )
    config.addinivalue_line(
        "markers", "large_download: marks tests that download large files (multi-GB)"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests that take a long time to run"
    )
    config.addinivalue_line(
        "markers", "download: marks tests that perform downloads"
    )


def pytest_collection_modifyitems(config, items):
    """Automatically mark tests based on their characteristics."""
    for item in items:
        # Mark tests that use network as integration tests
        if any(fixture in item.fixturenames for fixture in ['clinvar_downloader']):
            item.add_marker(pytest.mark.integration)
        
        # Mark tests with 'large' in name as potentially slow
        if 'large' in item.name.lower():
            item.add_marker(pytest.mark.slow)


@pytest.fixture(scope="session", autouse=True)
def enable_eliot_stdout():
    """Ensure Eliot logs are pretty-printed to stdout during the test session."""
    to_nice_stdout()
    yield
