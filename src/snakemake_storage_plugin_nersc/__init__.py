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
    """Settings for the NERSC storage plugin.

    logical_root:
        The logical root prefix that Snakemake will see in queries.
        Defaults to "/global" (NERSC convention).

    physical_root:
        The physical root on the filesystem that should actually be used
        for I/O. Defaults to "/dvs_ro" (NERSC read-only mirror).

    By overriding these in tests or other environments, the plugin can be
    used without requiring real /global or /dvs_ro mounts.
    """

    logical_root: Optional[str] = None
    physical_root: Optional[str] = None


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
        # We only accept absolute paths; further checks are done in _real_path.
        if not query.startswith("/"):
            return StorageQueryValidationResult(
                query=query,
                valid=False,
                reason="NERSC storage plugin only supports absolute paths.",
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
        # Cache roots from provider settings, with NERSC defaults.
        settings: StorageProviderSettings = self.provider.settings  # type: ignore[assignment]
        self._logical_root = (settings.logical_root or "/global").rstrip("/")
        self._physical_root = (settings.physical_root or "/dvs_ro").rstrip("/")

    # ---------- internal helpers ----------

    def _real_path(self) -> str:
        """Map the logical path to the actual physical filesystem path.

        The mapping is:

            <logical_root>/...  ->  <physical_root>/...

        with defaults /global -> /dvs_ro for NERSC. In tests or other
        environments, logical_root and physical_root can be overridden via
        StorageProviderSettings.
        """
        query = self.query

        logical_prefix = self._logical_root + "/"
        if query.startswith(logical_prefix):
            rel = query[len(self._logical_root) :]
            return os.path.join(self._physical_root, rel.lstrip("/"))

        # If the query does not start with the logical root, fall back to
        # using it as-is. This should not normally happen if queries are
        # validated and constructed consistently.
        return query

    def _logical_from_physical(self, physical_path: str) -> str:
        """Map a physical path back to the logical namespace for globbing."""
        physical_prefix = self._physical_root + os.sep
        if physical_path.startswith(physical_prefix):
            rel = physical_path[len(physical_prefix) :]
            return self._logical_root + "/" + rel.lstrip("/")
        return physical_path

    # ---------- inventory / metadata ----------

    async def inventory(self, cache: IOCacheStorageInterface):
        """Populate IOCache with existence and mtime information if available.

        The IOCache interface in snakemake-interface-storage-plugins 4.x does not
        expose setters here, so we simply perform the checks to ensure that
        inventory can be called without raising, and let Snakemake fall back to
        direct exists()/mtime() calls when needed.
        """
        # Just touch the path to ensure this method is side-effect free and
        # does not raise for existing/non-existing objects.
        _ = Path(self._real_path()).exists()
        return

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

        This plugin is read-only with respect to the underlying filesystem,
        so storing is not supported.
        """
        raise WorkflowError(
            "NERSC storage plugin is read-only; store_object is not supported."
        )

    @retry_decorator
    def remove(self):
        """Remove the object from the storage.

        This plugin is read-only with respect to the underlying filesystem,
        so removal is not supported.
        """
        raise WorkflowError(
            "NERSC storage plugin is read-only; remove is not supported."
        )

    # ---------- globbing ----------

    @retry_decorator
    def list_candidate_matches(self) -> Iterable[str]:
        """Return a list of candidate matches in the storage for the query.

        We interpret the query as a glob pattern under logical_root, map it to
        physical_root, and then map matches back to logical_root.
        """
        import glob

        pattern = self._real_path()
        matches: list[str] = []
        for path in glob.glob(pattern):
            logical = self._logical_from_physical(path)
            matches.append(logical)
        return matches
