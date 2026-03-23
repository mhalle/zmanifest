from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ._types import ManifestMetadata
from .path import ZPath


def _to_manifest_path(path: str) -> str:
    """Normalize a lookup path to match on-disk format.

    Accepts both ``"/arr/c/0"`` and ``"arr/c/0"``.
    Returns ``"/arr/c/0"`` (absolute) for manifests with ``/``-prefixed paths,
    or ``"arr/c/0"`` (bare) for legacy manifests.
    The archive row ``""`` passes through unchanged.
    """
    if path == "":
        return ""
    return str(ZPath(path))


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    """A single entry from the manifest (all columns except ``data``).

    Attributes:
        path: Absolute path in the archive (e.g. ``"/arr/c/0"``).
        size: Logical (decompressed) size in bytes — what the consumer
            receives after content decoding.
        content_size: Stored (compressed) size in bytes — what's on disk
            or over the wire. None if not compressed or not known.
        checksum: Content hash (git-sha1). When using ``compress=`` in
            the builder, this is the hash of the data before compression.
            Otherwise, it's the hash of the bytes as stored.
        content_encoding: Transport-level compression (e.g. ``"deflate"``,
            ``"zstd"``). The resolve pipeline decompresses automatically.
            See :class:`~zmanifest.ContentEncoding`.
    """

    path: str
    size: int
    addressing: str = ""
    content_size: int | None = None
    checksum: str | None = None
    text: str | None = None
    resolve: str | None = None  # JSON string
    base_resolve: str | None = None  # JSON string
    id: str | None = None
    content_type: str | None = None
    content_encoding: str | None = None
    source: str | None = None
    metadata: str | None = None  # JSON string


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

    def __init__(self, source: str | bytes, *, eager_data: bool | None = None) -> None:
        import io
        if isinstance(source, bytes):
            self._pf = pq.ParquetFile(io.BytesIO(source))
            self._path = None
        else:
            self._pf = pq.ParquetFile(source)
            self._path = source
        self._metadata = self._parse_metadata()

        all_columns = self._pf.schema_arrow.names
        self._has_data_column = "data" in all_columns
        self._has_data_z_column = "data_z" in all_columns

        # Try fast path: load index from root row (row 0)
        if self._try_load_index():
            return

        # Fallback: eager load of all non-data columns
        if eager_data is None:
            total = sum(
                self._pf.metadata.row_group(i).total_byte_size
                for i in range(self._pf.metadata.num_row_groups)
            )
            eager_data = total < 256 * 1024 * 1024

        has_any_data = self._has_data_column or self._has_data_z_column
        if eager_data and has_any_data:
            self._table = self._pf.read()
            self._data_table: pa.Table | None = self._table
        else:
            load_columns = [c for c in all_columns if c not in ("data", "data_z")]
            self._table = self._pf.read(columns=load_columns)
            self._data_table = None

        # Build path -> row index mapping
        # Normalize to absolute paths (old files may have bare paths)
        path_col = self._table.column("path")
        self._index: dict[str, int] = {}
        for i, p in enumerate(path_col):
            raw = p.as_py()
            self._index[_to_manifest_path(raw)] = i

        # Build id -> row index mapping (sparse — most rows have no id)
        self._id_index: dict[str, int] = {}
        if "id" in self._table.column_names:
            id_col = self._table.column("id")
            for i, v in enumerate(id_col):
                val = v.as_py()
                if val is not None:
                    self._id_index[val] = i

    def _try_load_index(self) -> bool:
        """Try to load from the index row (row 0 with I flag).

        Returns True if the index was loaded, False to fall back to
        full table scan.
        """
        from ._types import Addressing

        # The last row group contains just the index row.
        # The second-to-last row group contains non-data rows (text, refs, etc).
        num_rgs = self._pf.metadata.num_row_groups
        if num_rgs == 0:
            return False

        # Read the last row group (index row only)
        last_rg_idx = num_rgs - 1
        try:
            index_rg = self._pf.read_row_groups(
                [last_rg_idx], columns=["path", "addressing", "text", "metadata"],
            )
        except Exception:
            return False

        if len(index_rg) != 1:
            return False

        path_idx = index_rg.column("path")[0].as_py()
        addr_idx = index_rg.column("addressing")[0].as_py()
        if path_idx != "" or addr_idx is None or Addressing.INDEX not in addr_idx:
            return False

        index_text = index_rg.column("text")[0].as_py()
        if index_text is None:
            return False

        try:
            index_data = json.loads(index_text)
        except (json.JSONDecodeError, TypeError):
            return False

        # Store the root row's metadata
        self._root_metadata_raw = index_rg.column("metadata")[0].as_py()

        # Read non-data row groups (between data rows and the index).
        # Data rows each get their own 1-row group. Count them from the index.
        rg_entries: dict[str, dict[str, Any]] = {}
        n_data_rgs = sum(
            1 for e in index_data if "D" in e.get("addressing", [])
        )
        non_data_rg_indices = list(range(n_data_rgs, num_rgs - 1))

        if non_data_rg_indices:
            try:
                non_data_rg = self._pf.read_row_groups(non_data_rg_indices)
                col_names = non_data_rg.column_names
                for i in range(len(non_data_rg)):
                    p = non_data_rg.column("path")[i].as_py()
                    row_dict: dict[str, Any] = {}
                    for col in col_names:
                        if col != "path" and col != "data":
                            row_dict[col] = non_data_rg.column(col)[i].as_py()
                    rg_entries[p] = row_dict
            except Exception:
                pass

        # Compute the absolute row number of the index row
        total_rows = sum(
            self._pf.metadata.row_group(i).num_rows for i in range(num_rgs)
        )
        root_row_num = total_rows - 1

        # Build entries and indexes from the index JSON
        self._indexed_entries: dict[str, ManifestEntry] = {}
        self._indexed_row_map: dict[str, int] = {}  # path -> parquet row number
        self._indexed_metadata: dict[int, str] = {}  # row number -> metadata JSON
        self._index: dict[str, int] = {}
        self._id_index: dict[str, int] = {}
        self._table = None
        self._data_table = None

        # Root row metadata
        if self._root_metadata_raw is not None:
            self._indexed_metadata[root_row_num] = self._root_metadata_raw

        for entry_dict in index_data:
            raw_path = entry_dict["p"]
            path = _to_manifest_path(raw_path)
            row_num = entry_dict["r"]
            addressing = entry_dict.get("a", "")
            # Non-data row group has full entry details
            rg = rg_entries.get(raw_path, {}) or rg_entries.get(path, {})
            entry = ManifestEntry(
                path=path,
                size=rg.get("size") or 0,
                addressing=addressing,
                content_size=rg.get("content_size"),
                checksum=rg.get("checksum"),
                text=rg.get("text"),
                resolve=rg.get("resolve"),
                base_resolve=rg.get("base_resolve"),
                id=rg.get("id"),
                content_type=rg.get("content_type"),
                content_encoding=rg.get("content_encoding"),
                source=rg.get("source"),
                metadata=rg.get("metadata"),
            )
            self._indexed_entries[path] = entry
            self._indexed_row_map[path] = row_num
            self._index[path] = row_num
            meta_raw = rg.get("metadata")
            if meta_raw is not None:
                self._indexed_metadata[row_num] = meta_raw
            eid = rg.get("id")
            if eid is not None:
                self._id_index[eid] = row_num

        # Root/index row last in iteration order
        self._index[""] = root_row_num
        return True

    # Keys whose values are JSON objects/arrays and should be parsed
    _JSON_METADATA_KEYS = {"base_resolve"}

    def _parse_metadata(self) -> ManifestMetadata:
        kv = self._pf.schema_arrow.metadata or {}
        result: dict[str, Any] = {}
        for k_bytes, v_bytes in kv.items():
            key = k_bytes.decode("utf-8") if isinstance(k_bytes, bytes) else k_bytes
            val = v_bytes.decode("utf-8") if isinstance(v_bytes, bytes) else v_bytes
            # Only JSON-decode known structured fields
            if key in self._JSON_METADATA_KEYS:
                try:
                    result[key] = json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    result[key] = val
            else:
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
    def archive_metadata(self) -> dict[str, Any] | None:
        """Metadata about the archive itself (from the ``""`` row), or None.

        This is metadata about the container (e.g. DICOM series UID,
        description, provenance) — not about any path within the archive.
        """
        return self.path_metadata("")

    # Keep old name as alias
    root_metadata = archive_metadata

    def path_metadata(self, path: str) -> dict[str, Any] | None:
        """Metadata dict from a path annotation row, or None.

        For archive-level metadata, use :attr:`archive_metadata`.
        For group/directory metadata, pass the path (e.g. ``"/scans/ct"``).
        """
        path = _to_manifest_path(path)
        idx = self._index.get(path)
        if idx is None:
            return None
        return self._row_metadata(idx)

    def _row_metadata(self, idx: int) -> dict[str, Any] | None:
        raw: str | None = None
        if self._table is None:
            # Index path
            raw = self._indexed_metadata.get(idx)
        else:
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
            idx = self._index.get(_to_manifest_path(path))
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
        if hasattr(self, "_indexed_entries"):
            # Find by row number
            for path, row_num in self._indexed_row_map.items():
                if row_num == idx:
                    return self._indexed_entries[path]
            return None
        return self._entry_at(idx)

    def has(self, path: str) -> bool:
        return _to_manifest_path(path) in self._index

    def has_id(self, id: str) -> bool:
        return id in self._id_index

    def _entry_at(self, idx: int) -> ManifestEntry:
        t = self._table
        if t is None:
            raise RuntimeError("No table loaded — use get_entry() for indexed manifests")
        addr = _scalar(t, "addressing", idx)
        raw_path = _scalar(t, "path", idx)
        return ManifestEntry(
            path=_to_manifest_path(raw_path) if raw_path else "",
            size=_scalar(t, "size", idx) or 0,
            addressing=addr if addr is not None else "",
            content_size=_scalar(t, "content_size", idx),
            checksum=_scalar(t, "checksum", idx),
            text=_scalar(t, "text", idx),
            resolve=_scalar(t, "resolve", idx),
            base_resolve=_scalar(t, "base_resolve", idx),
            id=_scalar(t, "id", idx),
            content_type=_scalar(t, "content_type", idx),
            content_encoding=_scalar(t, "content_encoding", idx),
            source=_scalar(t, "source", idx),
            metadata=_scalar(t, "metadata", idx),
        )

    def get_entry(self, path: str) -> ManifestEntry | None:
        path = _to_manifest_path(path)
        # Index fast path
        if hasattr(self, "_indexed_entries"):
            return self._indexed_entries.get(path)
        idx = self._index.get(path)
        if idx is None:
            return None
        return self._entry_at(idx)

    def get_data(self, path: str) -> bytes | None:
        """Read inline binary data for a specific row.

        Checks both ``data`` (uncompressed) and ``data_z`` (compressed)
        columns. If data was loaded eagerly, this is an O(1) lookup.
        Otherwise, reads the specific row group from parquet on demand.
        """
        if not self._has_data_column and not self._has_data_z_column:
            return None
        path = _to_manifest_path(path)
        idx = self._index.get(path)
        if idx is None:
            return None

        # Fast path: data already in memory
        if self._data_table is not None:
            if self._has_data_column:
                val = self._data_table.column("data")[idx].as_py()
                if val is not None:
                    return val
            if self._has_data_z_column:
                val = self._data_table.column("data_z")[idx].as_py()
                if val is not None:
                    return val
            return None

        # Slow path: read from parquet per row group
        columns = []
        if self._has_data_column:
            columns.append("data")
        if self._has_data_z_column:
            columns.append("data_z")

        row_groups = self._pf.metadata.num_row_groups
        cumulative = 0
        for rg_idx in range(row_groups):
            rg_rows = self._pf.metadata.row_group(rg_idx).num_rows
            if cumulative + rg_rows > idx:
                local_idx = idx - cumulative
                rg_table = self._pf.read_row_groups(
                    [rg_idx], columns=columns,
                )
                for col in columns:
                    val = rg_table.column(col)[local_idx].as_py()
                    if val is not None:
                        return val
                return None
            cumulative += rg_rows
        return None

    def _is_annotation(self, path: str) -> bool:
        """Annotation rows: archive row ("") and folder entries (F flag)."""
        if path == "":
            return True
        entry = self.get_entry(path)
        if entry is not None:
            from ._types import Addressing
            return Addressing.FOLDER in entry.addressing
        return False

    def list_paths(self) -> Iterator[str]:
        yield from self._index

    def list_prefix(self, prefix: str) -> Iterator[str]:
        zprefix = ZPath(prefix) if prefix and prefix != "" else ZPath.ROOT
        for p in self._index:
            if p == "":
                continue
            zp = ZPath(p)
            if zp.is_equal_or_child_of(zprefix):
                yield p

    def list_dir(self, prefix: str) -> Iterator[str]:
        """List immediate children under a prefix (like a directory listing).

        For prefix ``"/"``, lists top-level entries.
        Annotation/folder rows are excluded from iteration.

        Yields child names (not full paths): files as ``"name"``,
        subdirectories as ``"name/"``.
        """
        zprefix = ZPath(prefix) if prefix and prefix != "" else ZPath.ROOT

        seen: set[str] = set()
        for p in self._index:
            if p == "" or self._is_annotation(p):
                continue
            zp = ZPath(p)
            child = zp.child_name_under(zprefix)
            if child is None:
                continue
            # Check if child is a leaf or has deeper entries
            rel = zp.relative_to(zprefix)
            is_dir = "/" in rel
            entry = child + "/" if is_dir else child
            if entry not in seen:
                seen.add(entry)
                yield entry
