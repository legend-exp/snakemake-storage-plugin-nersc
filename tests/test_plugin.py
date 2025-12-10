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

    def get_query(self, tmp_path) -> str:
        # Simulate /dvs_ro and /global under tmp_path.
        dvs_root = tmp_path / "dvs_ro"
        global_root = tmp_path / "global"
        dvs_root.mkdir()
        global_root.mkdir()

        # Create a file under the simulated /dvs_ro.
        rel_path = os.path.join("cfs", "cdirs", "myproject", "data", "test.txt")
        real_path = dvs_root / rel_path
        real_path.parent.mkdir(parents=True, exist_ok=True)
        real_path.write_text("hello nersc")

        # Logical query that Snakemake would see.
        query = "/" + os.path.join("global", rel_path)

        # Expose the simulated roots via environment variables so that
        # tests (or manual runs) can mount them appropriately if needed.
        os.environ["NERSC_TEST_DVS_ROOT"] = str(dvs_root)
        os.environ["NERSC_TEST_GLOBAL_ROOT"] = str(global_root)

        return query

    def get_query_not_existing(self, tmp_path) -> str:
        # A path that we do not create under the simulated /dvs_ro.
        rel_path = os.path.join("cfs", "cdirs", "myproject", "data", "does_not_exist.txt")
        return "/" + os.path.join("global", rel_path)

    def get_storage_provider_cls(self) -> Type[StorageProviderBase]:
        # Return the StorageProvider class of this plugin
        return StorageProvider

    def get_storage_provider_settings(self) -> Optional[StorageProviderSettingsBase]:
        # No special settings required for this plugin.
        return StorageProviderSettings()
