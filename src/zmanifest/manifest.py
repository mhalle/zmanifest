from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ._types import ManifestMetadata


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    """A single entry from the manifest (all columns except `data`)."""

    path: str
    size: int
    addressing: list[str] = field(default_factory=list)
    content_size: int | None = None
    retrieval_key: str | None = None
    text: str | None = None
    external_uri: str | None = None
    offset: int | None = None
    length: int | None = None
    array_path: str | None = None
    chunk_key: str | None = None
    media_type: str | None = None
    source: str | None = None
    base_uri: str | None = None


def _scalar(table: pa.Table, col: str, idx: int) -> Any:
    """Extract a Python scalar from a table column at a given row index."""
    if col not in table.column_names:
        return None
    val = table.column(col)[idx].as_py()
    return val


class Manifest:
    """Wraps a ZMP parquet file with an in-memory index for O(1) path lookups.

    All columns except ``data`` are loaded eagerly. The ``data`` column is
    read on demand per-row to avoid loading large binary blobs into memory.
    """

    def __init__(self, path: str, *, eager_data: bool | None = None) -> None:
        self._pf = pq.ParquetFile(path)
        self._path = path

        all_columns = self._pf.schema_arrow.names
        self._has_data_column = "data" in all_columns

        # Decide whether to eagerly load the data column.
        # Default: eager if file is < 256 MB, lazy otherwise.
        if eager_data is None:
            file_size = self._pf.metadata.serialized_size
            # serialized_size may undercount; use row group sizes as proxy
            total = sum(
                self._pf.metadata.row_group(i).total_byte_size
                for i in range(self._pf.metadata.num_row_groups)
            )
            eager_data = total < 256 * 1024 * 1024

        if eager_data and self._has_data_column:
            # Load everything including data
            self._table = self._pf.read()
            self._data_table: pa.Table | None = self._table
        else:
            # Load all columns except data
            load_columns = [c for c in all_columns if c != "data"]
            self._table = self._pf.read(columns=load_columns)
            self._data_table = None

        # Build path -> row index mapping
        path_col = self._table.column("path")
        self._index: dict[str, int] = {}
        for i, p in enumerate(path_col):
            self._index[p.as_py()] = i

        # Build id -> row index mapping (sparse — most rows have no id)
        self._id_index: dict[str, int] = {}
        if "id" in self._table.column_names:
            id_col = self._table.column("id")
            for i, v in enumerate(id_col):
                val = v.as_py()
                if val is not None:
                    self._id_index[val] = i

        # Parse file-level metadata
        self._metadata = self._parse_metadata()

    def _parse_metadata(self) -> ManifestMetadata:
        kv = self._pf.schema_arrow.metadata or {}
        result: dict[str, Any] = {}
        for k_bytes, v_bytes in kv.items():
            key = k_bytes.decode("utf-8") if isinstance(k_bytes, bytes) else k_bytes
            val = v_bytes.decode("utf-8") if isinstance(v_bytes, bytes) else v_bytes
            # Try to JSON-decode the value
            try:
                result[key] = json.loads(val)
            except (json.JSONDecodeError, TypeError):
                result[key] = val
        # Extract required keys
        meta = ManifestMetadata(
            zmp_version=result.get("zmp_version", ""),
            zarr_format=result.get("zarr_format", ""),
            retrieval_scheme=result.get("retrieval_scheme", ""),
        )
        # Collect extra keys
        extra = {
            k: v
            for k, v in result.items()
            if k not in ("zmp_version", "zarr_format", "retrieval_scheme")
            and k != "pandas"  # ignore pandas metadata if present
        }
        if extra:
            meta["extra"] = extra
        return meta

    def __getitem__(self, path: str) -> ManifestEntry:
        """Look up an entry by path. Raises ``KeyError`` if not found."""
        entry = self.get_entry(path)
        if entry is None:
            raise KeyError(path)
        return entry

    def __contains__(self, path: str) -> bool:
        return self.has(path)

    def __len__(self) -> int:
        return len(self._index)

    class _IdAccessor:
        """Accessor for looking up entries by id: ``manifest.by_id["slice_42"]``."""

        def __init__(self, manifest: Manifest) -> None:
            self._manifest = manifest

        def __getitem__(self, id: str) -> ManifestEntry:
            entry = self._manifest.get_entry_by_id(id)
            if entry is None:
                raise KeyError(id)
            return entry

        def __contains__(self, id: str) -> bool:
            return self._manifest.has_id(id)

    @property
    def by_id(self) -> _IdAccessor:
        """Access entries by id: ``manifest.by_id["slice_42"]``."""
        return self._IdAccessor(self)

    @property
    def metadata(self) -> ManifestMetadata:
        return self._metadata

    @property
    def root_metadata(self) -> dict[str, Any] | None:
        """Dataset-level metadata from the root row (path ``""``), or None."""
        return self.path_metadata("")

    def path_metadata(self, path: str) -> dict[str, Any] | None:
        """Metadata dict from an annotation row, or None.

        Use ``""`` for root, or a trailing-slash path for groups
        (e.g. ``"temperature/"``).
        """
        if path != "" and not path.endswith("/"):
            path = path + "/"
        idx = self._index.get(path)
        if idx is None:
            return None
        return self._row_metadata(idx)

    def _row_metadata(self, idx: int) -> dict[str, Any] | None:
        raw = _scalar(self._table, "metadata", idx)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None

    def get_metadata(self, *, path: str | None = None, id: str | None = None) -> dict[str, Any] | None:
        """Get the per-entry metadata dict by path or id.

        For annotation rows (root or group), use :meth:`path_metadata`
        or :attr:`root_metadata` instead. This method works on any row.

        Args:
            path: Look up by path.
            id: Look up by id.

        Returns:
            Parsed metadata dict, or None.
        """
        if id is not None:
            idx = self._id_index.get(id)
        elif path is not None:
            idx = self._index.get(path)
        else:
            return None
        if idx is None:
            return None
        return self._row_metadata(idx)

    def get_entry_by_id(self, id: str) -> ManifestEntry | None:
        """Look up an entry by its ``id`` column value."""
        idx = self._id_index.get(id)
        if idx is None:
            return None
        return self._entry_at(idx)

    def has(self, path: str) -> bool:
        return path in self._index

    def has_id(self, id: str) -> bool:
        return id in self._id_index

    def _entry_at(self, idx: int) -> ManifestEntry:
        t = self._table
        addr = _scalar(t, "addressing", idx)
        return ManifestEntry(
            path=_scalar(t, "path", idx),
            size=_scalar(t, "size", idx),
            addressing=addr if addr is not None else [],
            content_size=_scalar(t, "content_size", idx),
            retrieval_key=_scalar(t, "retrieval_key", idx),
            text=_scalar(t, "text", idx),
            external_uri=_scalar(t, "external_uri", idx),
            offset=_scalar(t, "offset", idx),
            length=_scalar(t, "length", idx),
            array_path=_scalar(t, "array_path", idx),
            chunk_key=_scalar(t, "chunk_key", idx),
            media_type=_scalar(t, "media_type", idx),
            source=_scalar(t, "source", idx),
            base_uri=_scalar(t, "base_uri", idx),
        )

    def get_entry(self, path: str) -> ManifestEntry | None:
        idx = self._index.get(path)
        if idx is None:
            return None
        return self._entry_at(idx)

    def get_data(self, path: str) -> bytes | None:
        """Read the ``data`` column for a specific row.

        If data was loaded eagerly, this is an O(1) lookup. Otherwise,
        reads the specific row group from the parquet file on demand.
        """
        if not self._has_data_column:
            return None
        idx = self._index.get(path)
        if idx is None:
            return None

        # Fast path: data already in memory
        if self._data_table is not None:
            return self._data_table.column("data")[idx].as_py()

        # Slow path: read from parquet per row group
        row_groups = self._pf.metadata.num_row_groups
        cumulative = 0
        for rg_idx in range(row_groups):
            rg_rows = self._pf.metadata.row_group(rg_idx).num_rows
            if cumulative + rg_rows > idx:
                local_idx = idx - cumulative
                rg_table = self._pf.read_row_groups(
                    [rg_idx], columns=["data"]
                )
                val = rg_table.column("data")[local_idx].as_py()
                return val
            cumulative += rg_rows
        return None

    @staticmethod
    def _is_annotation(path: str) -> bool:
        """Annotation rows: root ("") and path metadata ("group/")."""
        return path == "" or path.endswith("/")

    def list_paths(self) -> Iterator[str]:
        yield from self._index

    def list_prefix(self, prefix: str) -> Iterator[str]:
        for p in self._index:
            if p.startswith(prefix):
                yield p

    def list_dir(self, prefix: str) -> Iterator[str]:
        """List immediate children under a prefix (like a directory listing).

        For prefix ``""``, lists top-level entries.
        For prefix ``"group/"``, lists entries directly in that group.

        Annotation rows (root ``""`` and path metadata ``"group/"``) are
        excluded from iteration.
        """
        if prefix and not prefix.endswith("/"):
            prefix = prefix + "/"

        seen: set[str] = set()
        for p in self._index:
            if self._is_annotation(p):
                continue
            if not p.startswith(prefix):
                continue
            rest = p[len(prefix) :]
            slash_idx = rest.find("/")
            entry = rest if slash_idx == -1 else rest[: slash_idx + 1]
            if entry not in seen:
                seen.add(entry)
                yield entry
