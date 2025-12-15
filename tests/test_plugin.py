import os
from pathlib import Path
from typing import Optional, Type

import pytest
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_interface_storage_plugins.storage_provider import StorageProviderBase
from snakemake_interface_storage_plugins.tests import TestStorageBase

from snakemake_storage_plugin_nersc import (
    StorageProvider,
    StorageProviderSettings,
)


class TestStorage(TestStorageBase):
    __test__ = True
    # set to True if the storage is read-only
    retrieve_only = True
    # set to True if the storage is write-only
    store_only = False
    # set to False if the storage does not support deletion
    delete = False
    # set to True if the storage object implements support for touching (inherits from
    # StorageObjectTouch)
    touch = False
    # set to False if also directory upload/download should be tested (if your plugin
    # supports directory down-/upload, definitely do that)
    files_only = True

    @pytest.fixture(autouse=True)
    def _init_test_roots(self, tmp_path: Path):
        """Initialize per-test roots under pytest's tmp_path.

        The base test harness may call get_storage_provider_settings() before
        get_query(), so we must ensure the roots are available early.
        """
        self._test_physical_root = (tmp_path / "dvs_ro").resolve()
        self._test_logical_root = (tmp_path / "global").resolve()

        self._test_physical_root.mkdir(parents=True, exist_ok=True)
        self._test_logical_root.mkdir(parents=True, exist_ok=True)

        yield

    def get_query(self, tmp_path) -> str:
        # Create a file under the simulated physical root.
        rel_path = os.path.join("cfs", "cdirs", "myproject", "data", "test.txt")
        real_path = self._test_physical_root / rel_path
        real_path.parent.mkdir(parents=True, exist_ok=True)
        real_path.write_text("hello nersc")

        # Return a logical query under the simulated logical root.
        return str(self._test_logical_root / rel_path)

    def get_query_not_existing(self, tmp_path) -> str:
        rel_path = os.path.join(
            "cfs", "cdirs", "myproject", "data", "does_not_exist.txt"
        )
        return str(self._test_logical_root / rel_path)

    def get_storage_provider_cls(self) -> Type[StorageProviderBase]:
        # Return the StorageProvider class of this plugin
        return StorageProvider

    def get_storage_provider_settings(self) -> Optional[StorageProviderSettingsBase]:
        # Use tmp_path-based roots initialized by the autouse fixture.
        return StorageProviderSettings(
            logical_root=str(self._test_logical_root),
            physical_ro_root=str(self._test_physical_root),
        )
