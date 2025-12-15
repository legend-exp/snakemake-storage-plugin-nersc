from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, List, Optional

from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_storage_plugins.io import IOCacheStorageInterface
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_interface_storage_plugins.storage_object import (
    StorageObjectGlob,
    StorageObjectRead,
    retry_decorator,
)
from snakemake_interface_storage_plugins.storage_provider import (
    ExampleQuery,
    Operation,
    StorageProviderBase,
    StorageQueryValidationResult,
)
from snakemake_interface_storage_plugins.io import get_constant_prefix


@dataclass
class StorageProviderSettings(StorageProviderSettingsBase):
    """Settings for the NERSC storage plugin.

    logical_root:
        The logical root prefix that Snakemake will see in queries.
        Defaults to "/global" (NERSC convention).

    physical_ro_root:
        The physical *read-only* root that should be used for operations where
        the read-only mount is significantly more performant (e.g. globbing).
        Defaults to "/dvs_ro" (NERSC read-only mirror).

    By overriding these in tests or other environments, the plugin can be
    used without requiring real /global or /dvs_ro mounts.
    """

    logical_root: Optional[str] = None
    physical_ro_root: Optional[str] = None


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
        # We only accept absolute paths; further checks are done in _to_read_only.
        if not query.startswith("/"):
            return StorageQueryValidationResult(
                query=query,
                valid=False,
                reason="NERSC storage plugin only supports absolute paths.",
            )
        return StorageQueryValidationResult(query=query, valid=True)

    def postprocess_query(self, query: str) -> str:
        # Normalize the query path (POSIX semantics, without filesystem access).
        return str(PurePosixPath(query))

    def safe_print(self, query: str) -> str:
        """Process the query to remove potentially sensitive information when printing."""
        # No sensitive information in this simple implementation.
        return str(PurePosixPath(query))


class StorageObject(StorageObjectRead, StorageObjectGlob):
    # Do not override __init__; use __post_init__ instead.

    def __post_init__(self):
        # Cache roots from provider settings, with NERSC defaults.
        settings: StorageProviderSettings = self.provider.settings  # type: ignore[assignment]
        self._logical_root = PurePosixPath(settings.logical_root or "/global")
        self._physical_ro_root = PurePosixPath(settings.physical_ro_root or "/dvs_ro")

    def _to_read_only(self) -> Path:
        """Map the logical query path to the physical read-only root."""
        query = PurePosixPath(self.query)

        try:
            rel = query.relative_to(self._logical_root)
        except ValueError:
            # If the query does not start with the logical root, fall back to
            # using it as-is. This should not normally happen if queries are
            # validated and constructed consistently.
            return Path(str(query))

        return Path(str(self._physical_ro_root / rel))

    def _to_original(self, path: str) -> str:
        """Map a physical path back to the logical namespace for globbing."""
        physical = PurePosixPath(path)

        try:
            rel = physical.relative_to(self._physical_ro_root)
        except ValueError:
            return str(physical)

        return str(self._logical_root / rel)

    async def inventory(self, cache: IOCacheStorageInterface):
        """Populate IOCache with existence and mtime information if available.

        The IOCache interface in snakemake-interface-storage-plugins 4.x does not
        expose setters here, so we simply perform the checks to ensure that
        inventory can be called without raising, and let Snakemake fall back to
        direct exists()/mtime() calls when needed.
        """
        return None

    def get_inventory_parent(self) -> Optional[str]:
        """Return the parent directory of this object."""
        return str(self._to_read_only().parent)

    def local_suffix(self) -> str:
        """Return a unique suffix for the local path, determined from self.query."""
        # Use the logical query as suffix; this keeps local paths readable.
        return self.query

    def cleanup(self):
        """Perform local cleanup of any remainders of the storage object."""
        # Nothing special to do; Snakemake handles removal of local_path().
        return None

    @retry_decorator
    def exists(self) -> bool:
        return self._to_read_only().exists()

    @retry_decorator
    def mtime(self) -> float:
        real_path = self._to_read_only()
        try:
            return real_path.stat().st_mtime
        except FileNotFoundError as e:
            raise WorkflowError(f"Object does not exist: {self.query}") from e

    @retry_decorator
    def size(self) -> int:
        real_path = self._to_read_only()
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

    @retry_decorator
    def retrieve_object(self):
        """Ensure that the object is accessible locally under self.local_path()."""
        src = self._to_read_only()
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
    def list_candidate_matches(self) -> Iterable[str]:
        """Return a list of candidate matches in the storage for the query."""
        # This is used by glob_wildcards() to find matches for wildcards in the query.
        # The method has to return concretized queries without any remaining wildcards.
        prefix = self._to_read_only(Path(get_constant_prefix(self.query)))
        if prefix.is_dir():
            return map(str, prefix.rglob("*"))
        else:
            return (prefix,)
