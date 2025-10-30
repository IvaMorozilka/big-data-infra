import os
import tempfile

import pytest

from app.core.database import KVDB
from app.core.persistence import Snapshotter
from app.core.storage import InMemoryStorage
from app.core.wal import FileWal


@pytest.fixture
def temp_files():
    with tempfile.TemporaryDirectory() as temp_dir:
        snapshot_path = os.path.join(temp_dir, "snapshot.json")
        wal_path = os.path.join(temp_dir, "wal.log")
        yield snapshot_path, wal_path


@pytest.fixture
def temp_snapshot_file():
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
        temp_path = f.name
    yield temp_path
    # Cleanup
    if os.path.exists(temp_path):
        os.remove(temp_path)


@pytest.fixture
def temp_wal_file():
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.log') as f:
        temp_path = f.name
    yield temp_path
    # Cleanup
    if os.path.exists(temp_path):
        os.remove(temp_path)


@pytest.fixture
def storage():
    return InMemoryStorage()


@pytest.fixture
def snapshotter(temp_snapshot_file):
    return Snapshotter(temp_snapshot_file)


@pytest.fixture
def wal(temp_wal_file):
    return FileWal(temp_wal_file)


@pytest.fixture
def db(temp_files):
    snapshot_path, wal_path = temp_files
    return KVDB(
        storage_engine=InMemoryStorage(),
        persistence=Snapshotter(snapshot_path),
        wal=FileWal(wal_path),
        auto_snapshot_threshold=100
    )


@pytest.fixture
def db_with_low_threshold(temp_files):
    snapshot_path, wal_path = temp_files
    return KVDB(
        storage_engine=InMemoryStorage(),
        persistence=Snapshotter(snapshot_path),
        wal=FileWal(wal_path),
        auto_snapshot_threshold=5
    )


def create_db(snapshot_path: str, wal_path: str, threshold: int = 100) -> KVDB:
    return KVDB(
        storage_engine=InMemoryStorage(),
        persistence=Snapshotter(snapshot_path),
        wal=FileWal(wal_path),
        auto_snapshot_threshold=threshold
    )
