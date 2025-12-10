from __future__ import annotations

from dataclasses import dataclass, field
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


# NOTE:
# This is currently implemented as a trivial "local filesystem" storage plugin
# that operates under the provider's local_prefix. It is meant as a working
# skeleton that passes the generic interface tests. You can later replace the
# internals with real NERSC logic while keeping the public behaviour.


@dataclass
class StorageProviderSettings(StorageProviderSettingsBase):
    # For now we don't need any custom settings. Keep a dummy optional parameter
    # to demonstrate how settings work, but make it non-required so tests pass
    # without CLI/env configuration.
    root: Optional[str] = field(
        default=None,
        metadata={
            "help": "Optional root directory for NERSC storage (for testing, a local path).",
            "env_var": False,
            "required": False,
        },
    )


class StorageProvider(StorageProviderBase):
    # Do not override __init__; use __post_init__ instead.

    def __post_init__(self):
        # Determine a base directory for all objects of this provider.
        # For now, use settings.root if given, otherwise the local_prefix
        # that Snakemake passes in (a temporary directory in tests).
        if self.settings and getattr(self.settings, "root", None):
            self.base_dir = Path(self.settings.root)
        else:
            self.base_dir = Path(self.local_prefix)

        self.base_dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def example_queries(cls) -> List[ExampleQuery]:
        """Return example queries with description for this storage provider."""
        return [
            ExampleQuery(
                query="example.txt",
                description="A file named 'example.txt' in the provider's base directory.",
            )
        ]

    def rate_limiter_key(self, query: str, operation: Operation) -> Any:
        """Return a key for identifying a rate limiter given a query and an operation."""
        # Local filesystem backend does not really need rate limiting; just
        # group everything under a single key.
        return "local"

    def default_max_requests_per_second(self) -> float:
        """Return the default maximum number of requests per second."""
        # No real rate limiting needed for local filesystem.
        return 0.0

    def use_rate_limiter(self) -> bool:
        """Return False if no rate limiting is needed for this provider."""
        return False

    @classmethod
    def is_valid_query(cls, query: str) -> StorageQueryValidationResult:
        """Return whether the given query is valid for this storage provider."""
        # For this simple implementation, accept any non-empty string.
        if not query:
            return StorageQueryValidationResult(
                valid=False,
                reason="Query must be a non-empty string.",
            )
        return StorageQueryValidationResult(valid=True)

    def postprocess_query(self, query: str) -> str:
        # For now, just normalize to a relative POSIX-style path.
        return str(Path(query))

    def safe_print(self, query: str) -> str:
        """Process the query to remove potentially sensitive information when printing."""
        # No sensitive information in this simple implementation.
        return query


class StorageObject(StorageObjectRead, StorageObjectWrite, StorageObjectGlob):
    # Do not override __init__; use __post_init__ instead.

    def __post_init__(self):
        # Resolve the absolute path of this object under the provider's base_dir.
        self.path = Path(self.provider.base_dir) / self.query

    async def inventory(self, cache: IOCacheStorageInterface):
        """Populate IOCache with existence and mtime information if available."""
        key = self.cache_key()
        if self.path.exists():
            cache.exists[key] = True
            try:
                cache.mtime[key] = self.path.stat().st_mtime
            except OSError:
                pass
        else:
            cache.exists[key] = False

    def get_inventory_parent(self) -> Optional[str]:
        """Return the parent directory of this object."""
        return str(self.path.parent)

    def local_suffix(self) -> str:
        """Return a unique suffix for the local path, determined from self.query."""
        # Use the query itself as suffix; this keeps local paths readable.
        return self.query

    def cleanup(self):
        """Perform local cleanup of any remainders of the storage object."""
        # Nothing special to do; Snakemake handles removal of local_path().
        return None

    @retry_decorator
    def exists(self) -> bool:
        return self.path.exists()

    @retry_decorator
    def mtime(self) -> float:
        try:
            return self.path.stat().st_mtime
        except FileNotFoundError as e:
            raise WorkflowError(f"Object does not exist: {self.query}") from e

    @retry_decorator
    def size(self) -> int:
        try:
            if self.path.is_dir():
                total = 0
                for p in self.path.rglob("*"):
                    if p.is_file():
                        total += p.stat().st_size
                return total
            return self.path.stat().st_size
        except FileNotFoundError as e:
            raise WorkflowError(f"Object does not exist: {self.query}") from e

    @retry_decorator
    def local_footprint(self) -> int:
        # For this simple implementation, local footprint equals size.
        return self.size()

    @retry_decorator
    def retrieve_object(self):
        """Ensure that the object is accessible locally under self.local_path()."""
        # For this local backend, "retrieval" is copying from provider path to
        # the Snakemake-managed local_path().
        src = self.path
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
        """Store the object from self.local_path() into the provider path."""
        src = Path(self.local_path())
        dst = self.path

        if not src.exists():
            raise WorkflowError(
                f"Local object to store does not exist: {self.local_path()}"
            )

        dst.parent.mkdir(parents=True, exist_ok=True)

        if src.is_dir():
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
    def remove(self):
        """Remove the object from the storage."""
        if not self.path.exists():
            return
        if self.path.is_dir():
            for p in sorted(self.path.rglob("*"), reverse=True):
                if p.is_file():
                    p.unlink()
                else:
                    p.rmdir()
            self.path.rmdir()
        else:
            self.path.unlink()

    @retry_decorator
    def list_candidate_matches(self) -> Iterable[str]:
        """Return a list of candidate matches in the storage for the query.

        For this simple implementation, we interpret the query as a glob pattern
        relative to the provider's base_dir.
        """
        base = Path(self.provider.base_dir)
        pattern = self.query

        # Use pathlib's glob to expand the pattern.
        matches: list[str] = []
        for p in base.glob(pattern):
            # Return queries relative to base_dir (no wildcards).
            rel = p.relative_to(base)
            matches.append(str(rel))

        return matches
