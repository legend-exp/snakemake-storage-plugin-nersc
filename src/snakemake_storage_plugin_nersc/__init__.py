from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, List, Optional

from snakemake_interface_common.exceptions import WorkflowError  # noqa
from snakemake_interface_storage_plugins.io import IOCacheStorageInterface
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_interface_storage_plugins.storage_object import (
    StorageObjectGlob,
    StorageObjectRead,
    StorageObjectWrite,
    retry_decorator,
)
from snakemake_interface_storage_plugins.storage_provider import (  # noqa
    ExampleQuery,
    Operation,
    StorageProviderBase,
    StorageQueryValidationResult,
)


@dataclass
class StorageProviderSettings(StorageProviderSettingsBase):
    # No custom settings for now.
    pass


class StorageProvider(StorageProviderBase):
    # Do not override __init__; use __post_init__ instead.

    def __post_init__(self):
        # Nothing to initialize for this simple mapping provider.
        pass

    @classmethod
    def example_queries(cls) -> List[ExampleQuery]:
        """Return example queries with description for this storage provider."""
        return [
            ExampleQuery(
                query="/global/cfs/cdirs/myproject/data/file.txt",
                description="File on NERSC global filesystem, accessed via /dvs_ro",
                type="file",
            )
        ]

    def rate_limiter_key(self, query: str, operation: Operation) -> Any:
        """Return a key for identifying a rate limiter given a query and an operation."""
        # Local filesystem backend does not really need rate limiting.
        return None

    def default_max_requests_per_second(self) -> float:
        """Return the default maximum number of requests per second."""
        # No rate limiting.
        return float("inf")

    def use_rate_limiter(self) -> bool:
        """Return False if no rate limiting is needed for this provider."""
        return False

    @classmethod
    def is_valid_query(cls, query: str) -> StorageQueryValidationResult:
        """Return whether the given query is valid for this storage provider."""
        if not query.startswith("/global/"):
            return StorageQueryValidationResult(
                query=query,
                valid=False,
                reason="NERSC storage plugin only supports paths starting with /global/",
            )
        return StorageQueryValidationResult(query=query, valid=True)

    def postprocess_query(self, query: str) -> str:
        # Normalize the query path.
        return os.path.normpath(query)

    def safe_print(self, query: str) -> str:
        """Process the query to remove potentially sensitive information when printing."""
        # No sensitive information in this simple implementation.
        return os.path.normpath(query)


class StorageObject(StorageObjectRead, StorageObjectWrite, StorageObjectGlob):
    # Do not override __init__; use __post_init__ instead.

    def __post_init__(self):
        # Nothing to initialize beyond what the base classes do.
        pass

    # ---------- internal helpers ----------

    def _real_path(self) -> str:
        """
        Map the logical /global/... path to the actual /dvs_ro/... path.

        For tests, allow overriding the roots via environment variables:
        NERSC_TEST_GLOBAL_ROOT and NERSC_TEST_DVS_ROOT.
        """
        query = self.query

        # Test override: map /global to a temporary root
        test_global_root = os.environ.get("NERSC_TEST_GLOBAL_ROOT")
        test_dvs_root = os.environ.get("NERSC_TEST_DVS_ROOT")
        if test_global_root and test_dvs_root and query.startswith("/global/"):
            # Replace the /global prefix with the test /dvs_ro root
            rel = query[len("/global") :]
            return os.path.join(test_dvs_root, rel.lstrip("/"))

        # Default mapping for real NERSC environment
        if query.startswith("/global/"):
            return "/dvs_ro" + query[len("/global") :]

        # Should not happen if validation is correct, but be defensive.
        return query

    # ---------- inventory / metadata ----------

    async def inventory(self, cache: IOCacheStorageInterface):
        """Populate IOCache with existence and mtime information if available."""
        key = self.cache_key()
        real_path = Path(self._real_path())
        exists = real_path.exists()
        cache.set_exists(key, exists)
        if exists:
            try:
                cache.set_mtime(key, real_path.stat().st_mtime)
            except OSError:
                # Ignore mtime errors; existence info is still useful.
                pass

    def get_inventory_parent(self) -> Optional[str]:
        """Return the parent directory of this object."""
        return str(Path(self._real_path()).parent)

    def local_suffix(self) -> str:
        """Return a unique suffix for the local path, determined from self.query."""
        # Use the logical query as suffix; this keeps local paths readable.
        return self.query

    def cleanup(self):
        """Perform local cleanup of any remainders of the storage object."""
        # Nothing special to do; Snakemake handles removal of local_path().
        return None

    # ---------- basic file info ----------

    @retry_decorator
    def exists(self) -> bool:
        return Path(self._real_path()).exists()

    @retry_decorator
    def mtime(self) -> float:
        real_path = Path(self._real_path())
        try:
            return real_path.stat().st_mtime
        except FileNotFoundError as e:
            raise WorkflowError(f"Object does not exist: {self.query}") from e

    @retry_decorator
    def size(self) -> int:
        real_path = Path(self._real_path())
        try:
            if real_path.is_dir():
                total = 0
                for p in real_path.rglob("*"):
                    if p.is_file():
                        total += p.stat().st_size
                return total
            return real_path.stat().st_size
        except FileNotFoundError as e:
            raise WorkflowError(f"Object does not exist: {self.query}") from e

    @retry_decorator
    def local_footprint(self) -> int:
        # For this simple implementation, local footprint equals size.
        return self.size()

    # ---------- data transfer (read-only) ----------

    @retry_decorator
    def retrieve_object(self):
        """Ensure that the object is accessible locally under self.local_path()."""
        src = Path(self._real_path())
        dst = Path(self.local_path())

        if not src.exists():
            raise WorkflowError(f"Cannot retrieve non-existing object: {self.query}")

        dst.parent.mkdir(parents=True, exist_ok=True)

        if src.is_dir():
            # Simple recursive copy for directories.
            for p in src.rglob("*"):
                rel = p.relative_to(src)
                target = dst / rel
                if p.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(p.read_bytes())
        else:
            dst.write_bytes(src.read_bytes())

    @retry_decorator
    def store_object(self):
        """Store the object from self.local_path() into the provider path.

        This plugin is read-only with respect to the underlying /dvs_ro
        filesystem, so storing is not supported.
        """
        raise WorkflowError(
            "NERSC storage plugin is read-only; store_object is not supported."
        )

    @retry_decorator
    def remove(self):
        """Remove the object from the storage.

        This plugin is read-only with respect to the underlying /dvs_ro
        filesystem, so removal is not supported.
        """
        raise WorkflowError(
            "NERSC storage plugin is read-only; remove is not supported."
        )

    # ---------- globbing ----------

    @retry_decorator
    def list_candidate_matches(self) -> Iterable[str]:
        """Return a list of candidate matches in the storage for the query.

        We interpret the query as a glob pattern under /global, map it to
        /dvs_ro, and then map matches back to /global.
        """
        import glob

        pattern = self._real_path()
        matches: list[str] = []
        for path in glob.glob(pattern):
            # Map back to /global/... for Snakemake's perspective.
            test_dvs_root = os.environ.get("NERSC_TEST_DVS_ROOT")
            test_global_root = os.environ.get("NERSC_TEST_GLOBAL_ROOT")

            logical: str
            if test_dvs_root and test_global_root and path.startswith(
                os.path.join(test_dvs_root, "")
            ):
                # Map back from test /dvs_ro root to /global
                rel = path[len(os.path.join(test_dvs_root, "")) :]
                logical = "/global/" + rel.lstrip("/")
            elif path.startswith("/dvs_ro/"):
                logical = "/global" + path[len("/dvs_ro") :]
            else:
                logical = path
            matches.append(logical)
        return matches
