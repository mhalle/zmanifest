"""Build ZMP parquet manifests from raw entries."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import rfc8785

from ._types import Addressing, compute_addressing


def canonical_json(text: str) -> str:
    """Canonicalize a JSON string via RFC 8785.

    Returns the canonical form as a string. Raises ``json.JSONDecodeError``
    if the input is not valid JSON.
    """
    return rfc8785.dumps(json.loads(text)).decode("utf-8")


def git_blob_hash(content: bytes) -> str:
    """Compute git blob SHA-1: SHA-1('blob <size>\\0<content>')."""
    header = f"blob {len(content)}\0".encode()
    return hashlib.sha1(header + content).hexdigest()




# ---------------------------------------------------------------------------
# Builder — row-level manifest builder
# ---------------------------------------------------------------------------


@dataclass
class _Row:
    path: str
    size: int
    id: str | None = None
    content_size: int | None = None
    checksum: str | None = None
    text: str | None = None
    data: bytes | None = None  # uncompressed binary (parquet compression=none)
    data_z: bytes | None = None  # compressible binary (parquet compression=zstd)
    resolve: str | None = None  # JSON string
    base_resolve: str | None = None  # JSON string
    content_type: str | None = None
    content_encoding: str | None = None
    source: str | None = None
    metadata: str | None = None  # JSON string
    is_mount: bool = False
    is_link: bool = False
    is_folder: bool = False
    is_index: bool = False

    @property
    def addressing(self) -> str:
        return compute_addressing(
            text=self.text,
            data=self.data,
            data_z=self.data_z,
            resolve=self.resolve,
            is_link=self.is_link,
            is_mount=self.is_mount,
            is_folder=self.is_folder,
            is_index=self.is_index,
        )


class Builder:
    """Build a ZMP manifest by adding entries directly.

    Unlike store-level builders, this class lets you construct a manifest
    row by row — useful when building from external sources (DICOM headers,
    kerchunk references, database queries).

    JSON content is canonicalized via RFC 8785 before hashing for
    deterministic git-sha1 retrieval keys.

    Example::

        builder = Builder()
        builder.add("zarr.json", text='{"zarr_format":3,"node_type":"group"}')
        builder.add("temp/zarr.json", text=array_meta_json)
        builder.add("temp/c/0/0", data=chunk_bytes)
        builder.add("temp/c/1/0", uri="s3://bucket/file.nc", offset=1024, length=4096)
        builder.write("output.zmp")

    Args:
        zarr_format: Zarr format version (``"2"`` or ``"3"``).
        retrieval_scheme: Retrieval scheme for file-level metadata.
        data_compression: Parquet compression for the ``data`` column.
        data_compression_level: Compression level for the ``data`` column.
        max_rows_per_group: Override adaptive row group sizing.
        metadata: Additional key-value pairs for file-level metadata.
    """

    def __init__(
        self,
        *,
        zarr_format: str = "3",
        data_compression: str = "none",
        data_compression_level: int | None = None,
        max_rows_per_group: int | None = None,
        metadata: dict[str, object] | None = None,
        base_resolve: dict | None = None,
    ) -> None:
        self._zarr_format = zarr_format
        self._data_compression = data_compression
        self._data_compression_level = data_compression_level
        self._max_rows_per_group = max_rows_per_group
        self._metadata = metadata or {}
        self._base_resolve = base_resolve
        self._rows: list[_Row] = []

    @staticmethod
    def _encode_metadata(metadata: dict[str, object] | None) -> str | None:
        if metadata is None:
            return None
        return rfc8785.dumps(metadata).decode("utf-8")

    def set_archive_metadata(
        self,
        metadata: dict[str, object],
        *,
        id: str | None = None,
    ) -> None:
        """Set metadata about the archive itself.

        This is metadata about the container (e.g. DICOM series UID,
        description, provenance) — not about any path within the archive.
        Stored on the ``""`` row which also holds the index.

        Calling this multiple times replaces the previous value.

        Args:
            metadata: Archive-level metadata dict.
            id: Optional short identifier for this row.
        """
        self.set_path_metadata("", metadata, id=id)

    # Keep old name as alias for backward compatibility
    set_root_metadata = set_archive_metadata

    def set_path_metadata(
        self,
        path: str,
        metadata: dict[str, object],
        *,
        id: str | None = None,
    ) -> None:
        """Set metadata on a path (group or directory) in the manifest.

        Use :meth:`set_archive_metadata` for archive-level metadata.
        Use this method for group/directory annotations
        (e.g. ``"temperature"``, ``"scans/ct"``).

        These rows are invisible to the zarr Store interface but queryable
        via DuckDB or :meth:`Manifest.path_metadata`.

        Args:
            path: Path for the metadata row, e.g. ``"scans/ct"``.
                Use :meth:`set_archive_metadata` for archive-level metadata.
            metadata: Metadata dict (stored as JSON).
            id: Optional short identifier for this row.
        """
        path = path.rstrip("/")
        # Remove any existing row for this path
        self._rows = [r for r in self._rows if r.path != path]
        self._rows.append(_Row(
            path=path,
            size=0,
            id=id,
            metadata=self._encode_metadata(metadata),
            is_folder=True,
        ))

    def mount(
        self,
        path: str,
        resolve: dict | None = None,
        *,
        data: bytes | None = None,
        data_z: bytes | None = None,
        id: str | None = None,
        content_type: str | None = None,
        base_resolve: dict | None = None,
        metadata: dict[str, object] | None = None,
    ) -> None:
        """Mount an external store at a path prefix.

        The mounted store is opened lazily on first access and handles
        all reads under this prefix. The child manifest can be referenced
        via ``resolve`` or embedded inline via ``data`` (uncompressed,
        e.g. a .zmp file) or ``data_z`` (compressible).

        Args:
            path: Mount point path (trailing ``/`` added if missing).
            resolve: Resolution dict (e.g. ``{"http": {"url": "child.zmp"}}``).
            data: Embedded child manifest (uncompressed parquet column).
            data_z: Embedded child manifest (zstd-compressed parquet column).
            id: Optional short identifier.
            content_type: MIME type hint.
            base_resolve: Default resolution params for entries within
                the mounted store, keyed by scheme.
            metadata: Per-entry metadata dict.
        """
        if data is not None and data_z is not None:
            raise ValueError("Cannot set both data and data_z")
        path = path.rstrip("/")
        size = len(data) if data else len(data_z) if data_z else 0
        # Remove any existing row for this path
        self._rows = [r for r in self._rows if r.path != path]
        self._rows.append(_Row(
            path=path,
            size=size,
            id=id,
            data=data,
            data_z=data_z,
            resolve=json.dumps(resolve, separators=(",", ":")) if resolve else None,
            base_resolve=json.dumps(base_resolve, separators=(",", ":")) if base_resolve else None,
            content_type=content_type,
            metadata=self._encode_metadata(metadata),
            is_mount=True,
            is_folder=True,
        ))

    def link(
        self,
        path: str,
        target: str,
        *,
        folder: bool = False,
        id: str | None = None,
        content_type: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> None:
        """Create a link entry that points to another path in the manifest.

        When resolved, the link's content comes from the target entry.
        If ``folder=True`` (or path ends with ``/``), the link acts as
        a directory — all sub-paths are rewritten through the target prefix.

        Args:
            path: The link's path in the manifest.
            target: Path of the target entry (relative to manifest root).
            folder: If True, this is a directory link.
            id: Optional short identifier.
            content_type: MIME type hint.
            metadata: Per-entry metadata dict.
        """
        # Detect directory link from trailing /
        if path.endswith("/"):
            folder = True
        path = path.rstrip("/")
        target = target.rstrip("/")
        resolve_dict = {"_path": {"target": target}}
        # Remove any existing row for this path
        self._rows = [r for r in self._rows if r.path != path]
        self._rows.append(_Row(
            path=path,
            size=0,
            id=id,
            resolve=json.dumps(resolve_dict, separators=(",", ":")),
            content_type=content_type,
            metadata=self._encode_metadata(metadata),
            is_link=True,
            is_folder=folder,
        ))

    def add(
        self,
        path: str,
        *,
        text: str | None = None,
        data: bytes | None = None,
        data_z: bytes | None = None,
        resolve: dict | None = None,
        size: int | None = None,
        content_size: int | None = None,
        checksum: str | None = None,
        id: str | None = None,
        content_type: str | None = None,
        content_encoding: str | None = None,
        source: str | None = None,
        base_resolve: dict | None = None,
        metadata: dict[str, object] | None = None,
    ) -> None:
        """Add an entry to the manifest.

        Supply one or more addressing sources. The ``addressing`` column
        is computed automatically from which fields are set.

        Args:
            path: Store path (e.g. ``"zarr.json"``, ``"arr/c/0/1"``).
            text: Inline text content.
            data: Inline binary content (stored uncompressed in parquet).
                Use for pre-compressed data (zarr chunks, .zmp files, etc.).
            data_z: Inline binary content (stored with zstd compression in
                parquet). Use for compressible raw data (uncompressed pixels, etc.).
            resolve: Resolution dict keyed by scheme
                (e.g. ``{"http": {"url": "..."}, "git": {"oid": "..."}}``).
            size: Size in bytes. Auto-computed from ``text``, ``data``,
                or ``data_z`` if not provided.
            content_size: Logical/decoded size in bytes (optional).
            checksum: Content hash for verification.
            id: Optional short identifier for cross-referencing.
            content_type: MIME type of the content (e.g. ``"application/json"``).
            content_encoding: Content encoding (e.g. ``"gzip"``, ``"zstd"``).
            source: Provenance string.
            base_resolve: Default resolution params for this entry's
                scheme-specific resolution, keyed by scheme.
            metadata: Per-entry metadata dict (stored as JSON).
        """
        if data is not None and data_z is not None:
            raise ValueError("Cannot set both data and data_z")

        # Canonicalize JSON text (by extension or content_type)
        if text is not None and (
            path.endswith(".json")
            or (content_type and "json" in content_type)
        ):
            try:
                text = rfc8785.dumps(json.loads(text)).decode("utf-8")
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass

        # Auto-compute size
        if size is None:
            if text is not None:
                size = len(text.encode("utf-8"))
            elif data is not None:
                size = len(data)
            elif data_z is not None:
                size = len(data_z)
            else:
                size = 0

        # Auto-compute checksum from content
        if checksum is None:
            if text is not None:
                checksum = git_blob_hash(text.encode("utf-8"))
            elif data is not None:
                checksum = git_blob_hash(data)
            elif data_z is not None:
                checksum = git_blob_hash(data_z)

        resolve_json = json.dumps(resolve, separators=(",", ":")) if resolve else None
        base_resolve_json = json.dumps(base_resolve, separators=(",", ":")) if base_resolve else None

        self._rows.append(_Row(
            path=path,
            size=size,
            id=id,
            content_size=content_size,
            checksum=checksum,
            text=text,
            data=data,
            data_z=data_z,
            resolve=resolve_json,
            base_resolve=base_resolve_json,
            content_type=content_type,
            content_encoding=content_encoding,
            source=source,
            metadata=self._encode_metadata(metadata),
        ))

    # Adaptive row group sizing defaults.
    # See docs/parquet-layout.md for benchmarks and rationale.
    _TARGET_RG_DATA_BYTES: int = 10 * 1024 * 1024  # 10 MB of blob data per RG
    _MAX_RG_ROWS: int = 2000  # cap rows per RG even for tiny blobs

    def write(self, output: str | Path) -> Path:
        """Write the manifest to a ZMP parquet file.

        Row groups are sized adaptively to keep blob-column I/O bounded:
        each row group accumulates rows until the data column would exceed
        ``TARGET_RG_DATA_BYTES`` (10 MB) or the row count exceeds
        ``MAX_RG_ROWS`` (2000). This adapts to mixed blob sizes
        automatically.

        Row order is deterministic: data rows (sorted by path), then
        non-data rows (sorted by path), then the archive row.

        Returns:
            Path to the written file.
        """
        output = Path(output)

        # Ensure the archive row ("") exists — holds archive-level metadata.
        # Not a path in the hierarchy.
        has_archive_row = any(r.path == "" for r in self._rows)
        if not has_archive_row:
            self._rows.append(_Row(path="", size=0, is_folder=True))

        # Canonicalize all JSON fields for deterministic output
        for r in self._rows:
            if r.resolve is not None:
                r.resolve = rfc8785.dumps(json.loads(r.resolve)).decode("utf-8")
            if r.base_resolve is not None:
                r.base_resolve = rfc8785.dumps(json.loads(r.base_resolve)).decode("utf-8")
            if r.metadata is not None:
                r.metadata = rfc8785.dumps(json.loads(r.metadata)).decode("utf-8")

        # Deterministic row order:
        # 1. data/data_z rows (sorted by path) — bulk of the file
        # 2. non-data rows (sorted by path) — metadata, resolve refs
        # 3. archive row (path "") — archive metadata
        def _has_data(r: _Row) -> bool:
            return r.data is not None or r.data_z is not None

        data_rows = sorted(
            [r for r in self._rows if _has_data(r)], key=lambda r: r.path
        )
        non_data_rows = sorted(
            [r for r in self._rows if not _has_data(r) and r.path != ""],
            key=lambda r: r.path,
        )
        archive_rows = [r for r in self._rows if r.path == ""]
        rows = data_rows + non_data_rows + archive_rows

        # Build the table
        def _col(attr: str) -> list[Any]:
            return [getattr(r, attr) for r in rows]

        table = pa.table(
            {
                "path": pa.array(_col("path"), type=pa.string()),
                "id": pa.array(_col("id"), type=pa.string()),
                "size": pa.array(_col("size"), type=pa.int64()),
                "content_size": pa.array(_col("content_size"), type=pa.int64()),
                "checksum": pa.array(_col("checksum"), type=pa.string()),
                "text": pa.array(_col("text"), type=pa.string()),
                "data": pa.array(_col("data"), type=pa.binary()),
                "data_z": pa.array(_col("data_z"), type=pa.binary()),
                "resolve": pa.array(_col("resolve"), type=pa.string()),
                "base_resolve": pa.array(_col("base_resolve"), type=pa.string()),
                "content_type": pa.array(_col("content_type"), type=pa.string()),
                "content_encoding": pa.array(_col("content_encoding"), type=pa.string()),
                "source": pa.array(_col("source"), type=pa.string()),
                "metadata": pa.array(_col("metadata"), type=pa.string()),
                "addressing": pa.array(
                    [r.addressing for r in rows], type=pa.string()
                ),
            }
        )

        # File-level metadata
        file_meta: dict[bytes, bytes] = {
            b"zmp_version": b"0.2.0",
            b"zarr_format": self._zarr_format.encode(),
        }
        if self._base_resolve is not None:
            file_meta[b"base_resolve"] = rfc8785.dumps(self._base_resolve)
        for k, v in self._metadata.items():
            if isinstance(v, str):
                file_meta[k.encode()] = v.encode()
            else:
                file_meta[k.encode()] = rfc8785.dumps(v)

        schema = table.schema.with_metadata(file_meta)
        table = table.cast(schema)

        # Per-column compression: data is uncompressed (pre-compressed
        # zarr chunks), everything else is zstd for compact metadata.
        compression = {col: "zstd" for col in table.schema.names}
        compression["data"] = "none"
        # Disable dictionary encoding for unique-value columns
        use_dictionary = {col: False for col in table.schema.names}

        compression_level = None
        if self._data_compression_level is not None:
            compression_level = {"data": self._data_compression_level}

        writer = pq.ParquetWriter(
            str(output),
            schema,
            compression=compression,
            compression_level=compression_level,
            use_dictionary=use_dictionary,
            write_page_index=True,
        )
        try:
            n_data = len(data_rows)
            n_non_data = len(non_data_rows)
            n_archive = len(archive_rows)

            if self._max_rows_per_group is not None:
                # User override: uniform row group sizing
                max_rg = self._max_rows_per_group
                n = len(table)
                i = 0
                while i < n:
                    end = min(i + max_rg, n)
                    writer.write_table(table.slice(i, end - i))
                    i = end
            else:
                # --- Adaptive row group sizing ---
                #
                # Data rows: accumulate until data column exceeds
                # TARGET_RG_DATA_BYTES or MAX_RG_ROWS.  This naturally
                # adapts to blob size: small blobs → large row groups,
                # large blobs → small row groups.
                target = self._TARGET_RG_DATA_BYTES
                max_rows = self._MAX_RG_ROWS
                rg_start = 0
                rg_data_bytes = 0
                rg_rows = 0

                for i in range(n_data):
                    blob_size = len(data_rows[i].data or b"") + len(data_rows[i].data_z or b"")
                    rg_data_bytes += blob_size
                    rg_rows += 1

                    if rg_data_bytes >= target or rg_rows >= max_rows:
                        writer.write_table(table.slice(rg_start, i - rg_start + 1))
                        rg_start = i + 1
                        rg_data_bytes = 0
                        rg_rows = 0

                # Flush remaining data rows
                if rg_start < n_data:
                    writer.write_table(table.slice(rg_start, n_data - rg_start))

                # Non-data rows: single row group (they're small)
                if n_non_data > 0:
                    writer.write_table(
                        table.slice(n_data, n_non_data)
                    )

                # Archive row: own row group (always last)
                if n_archive > 0:
                    writer.write_table(
                        table.slice(n_data + n_non_data, n_archive)
                    )
        finally:
            writer.close()

        return output
