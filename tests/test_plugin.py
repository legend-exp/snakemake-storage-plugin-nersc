import os
from pathlib import Path
from typing import Optional, Type

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

    # Use a test-local physical root under the repo / CWD so tests work anywhere.
    TEST_PHYSICAL_ROOT = Path("_nersc_test_dvs_ro").absolute()
    # Use a non-privileged logical root so the test harness can create dirs/files.
    TEST_LOGICAL_ROOT = str(Path("_nersc_test_global").absolute())

    def get_query(self, tmp_path) -> str:
        # Ensure physical root exists
        self.TEST_PHYSICAL_ROOT.mkdir(parents=True, exist_ok=True)

        # Create a file under the simulated physical root.
        rel_path = os.path.join("cfs", "cdirs", "myproject", "data", "test.txt")
        real_path = self.TEST_PHYSICAL_ROOT / rel_path
        real_path.parent.mkdir(parents=True, exist_ok=True)
        real_path.write_text("hello nersc")

        # Logical query that Snakemake would see.
        return str(Path(self.TEST_LOGICAL_ROOT) / rel_path)

    def get_query_not_existing(self, tmp_path) -> str:
        # A path that we do not create under the simulated physical root.
        rel_path = os.path.join(
            "cfs", "cdirs", "myproject", "data", "does_not_exist.txt"
        )
        return str(Path(self.TEST_LOGICAL_ROOT) / rel_path)

    def get_storage_provider_cls(self) -> Type[StorageProviderBase]:
        # Return the StorageProvider class of this plugin
        return StorageProvider

    def get_storage_provider_settings(self) -> Optional[StorageProviderSettingsBase]:
        # Configure plugin to map TEST_LOGICAL_ROOT → TEST_PHYSICAL_ROOT
        return StorageProviderSettings(
            logical_root=self.TEST_LOGICAL_ROOT,
            physical_ro_root=str(self.TEST_PHYSICAL_ROOT),
        )
