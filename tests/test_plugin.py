from pathlib import Path
from typing import Optional, Type

from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_interface_storage_plugins.storage_provider import StorageProviderBase
from snakemake_interface_storage_plugins.tests import TestStorageBase

from snakemake_storage_plugin_nersc import (
    StorageObject,
    StorageProvider,
    StorageProviderSettings,
)


class TestStorage(TestStorageBase):
    __test__ = True
    # set to True if the storage is read-only
    retrieve_only = False
    # set to True if the storage is write-only
    store_only = False
    # set to False if the storage does not support deletion
    delete = True
    # set to True if the storage object implements support for touching (inherits from
    # StorageObjectTouch)
    touch = False
    # set to False if also directory upload/download should be tested (if your plugin
    # supports directory down-/upload, definitely do that)
    files_only = True

    def get_query(self, tmp_path) -> str:
        # Use a simple filename relative to the provider's base directory.
        return "testfile.txt"

    def get_query_not_existing(self, tmp_path) -> str:
        # A filename that is guaranteed not to exist initially.
        return "non_existing_file.txt"

    def get_storage_provider_cls(self) -> Type[StorageProviderBase]:
        # Return the StorageProvider class of this plugin
        return StorageProvider

    def get_storage_provider_settings(self) -> Optional[StorageProviderSettingsBase]:
        # For tests, point the provider's root to the pytest tmp_path so that
        # all operations happen in an isolated directory.
        return StorageProviderSettings(root=str(Path(tmp_path := Path.cwd()) / "test_root"))
