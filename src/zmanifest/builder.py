"""Build ZMP parquet manifests from raw entries."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import rfc8785

from ._types import Addressing, ContentEncoding, compute_addressing
from .path import ZPath
from .resolve import _encode_content


def _to_manifest_path(path: str) -> str:
    """Normalize a path for storage in the manifest.

    Accepts both ``"/arr/c/0"`` and ``"arr/c/0"``.
    Returns absolute form ``"/arr/c/0"`` for on-disk storage.
    The archive row ``""`` passes through unchanged.
    """
    if path == "":
        return ""
    return str(ZPath(path))
from .path import ZPath


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
# Internal row representation
# ---------------------------------------------------------------------------


@dataclass
class _Row:
    path: str
    size: int
    id: str | None = None
    content_size: int | None = None
    checksum: str | None = None
    text: str | None = None
    data: bytes | None = None
    data_z: bytes | None = None
    resolve: str | None = None
    base_resolve: str | None = None
    content_type: str | None = None
    content_encoding: str | None = None
    source: str | None = None
    metadata: str | None = None
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

    @property
    def has_data(self) -> bool:
        return self.data is not None or self.data_z is not None

    @property
    def blob_size(self) -> int:
        return len(self.data or b"") + len(self.data_z or b"")


# ---------------------------------------------------------------------------
# Parquet schema and writer helpers
# ---------------------------------------------------------------------------

_SCHEMA = pa.schema([
    ("path", pa.string()),
    ("id", pa.string()),
    ("size", pa.int64()),
    ("content_size", pa.int64()),
    ("checksum", pa.string()),
    ("text", pa.string()),
    ("data", pa.binary()),
    ("data_z", pa.binary()),
    ("resolve", pa.string()),
    ("base_resolve", pa.string()),
    ("content_type", pa.string()),
    ("content_encoding", pa.string()),
    ("source", pa.string()),
    ("metadata", pa.string()),
    ("addressing", pa.string()),
])


def _rows_to_table(rows: list[_Row], schema: pa.Schema) -> pa.Table:
    """Convert a list of _Row to a pyarrow Table."""
    def _col(attr: str) -> list[Any]:
        return [getattr(r, attr) for r in rows]

    return pa.table(
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
        },
        schema=schema,
    )


def _make_file_meta(
    zarr_format: str,
    metadata: dict[str, object],
    base_resolve: dict | None,
) -> dict[bytes, bytes]:
    """Build parquet file-level metadata dict."""
    file_meta: dict[bytes, bytes] = {
        b"zmp_version": b"0.2.0",
        b"zarr_format": zarr_format.encode(),
    }
    if base_resolve is not None:
        file_meta[b"base_resolve"] = rfc8785.dumps(base_resolve)
    for k, v in metadata.items():
        if isinstance(v, str):
            file_meta[k.encode()] = v.encode()
        else:
            file_meta[k.encode()] = rfc8785.dumps(v)
    return file_meta


def _make_writer(
    output: str | Path | Any,
    schema: pa.Schema,
    data_compression_level: int | None = None,
) -> pq.ParquetWriter:
    """Create a ParquetWriter with ZMP conventions.

    ``output`` can be a file path (str/Path) or a writable file-like
    object (e.g. ``io.BytesIO``).
    """
    compression = {col: "zstd" for col in schema.names}
    compression["data"] = "none"
    # Disable dictionary encoding for data columns (unique blobs;
    # dictionary is useless and prevents direct byte access).
    # Keep dictionary for low-cardinality columns (addressing, content_type, etc.).
    # Note: dict-style {col: False} is buggy in pyarrow — use a list
    # of columns that SHOULD use dictionary.
    use_dictionary = [
        col for col in schema.names if col != "data"
    ]

    compression_level = None
    if data_compression_level is not None:
        compression_level = {"data": data_compression_level}

    where = str(output) if isinstance(output, (str, Path)) else output
    return pq.ParquetWriter(
        where,
        schema,
        compression=compression,
        compression_level=compression_level,
        use_dictionary=use_dictionary,
        write_page_index=True,
    )


def _canonicalize_row(r: _Row) -> None:
    """Canonicalize JSON fields in-place for deterministic output."""
    if r.resolve is not None:
        r.resolve = rfc8785.dumps(json.loads(r.resolve)).decode("utf-8")
    if r.base_resolve is not None:
        r.base_resolve = rfc8785.dumps(json.loads(r.base_resolve)).decode("utf-8")
    if r.metadata is not None:
        r.metadata = rfc8785.dumps(json.loads(r.metadata)).decode("utf-8")


# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------


class Builder:
    """Build a ZMP manifest by adding entries.

    Supports two modes:

    **Batch mode** (small manifests)::

        builder = Builder()
        builder.add("zarr.json", text='{...}')
        builder.add("arr/c/0", data=chunk_bytes)
        builder.write("output.zmp")

    **Streaming mode** (large manifests — data rows stream to disk)::

        with Builder(output="output.zmp") as builder:
            builder.add("zarr.json", text='{...}')   # buffered (small)
            builder.add("arr/c/0", data=chunk_bytes)  # streamed to disk
            builder.add("arr/c/1", data=chunk_bytes)  # streamed to disk
        # close() flushes remaining data + writes non-data + archive row

    In streaming mode, data rows (those with ``data`` or ``data_z``) are
    written to the parquet file incrementally with adaptive row group
    sizing. Non-data rows (text, resolve refs, metadata) are buffered
    in memory (they're small) and written at close time.

    Row order: data rows (sorted within each row group), then non-data
    rows (sorted), then the archive row.

    Args:
        output: Path for the output ``.zmp`` file. If provided, enables
            streaming mode. If None, use :meth:`write` for batch mode.
        zarr_format: Zarr format version (``"2"`` or ``"3"``).
        data_compression: Parquet compression for the ``data`` column.
        data_compression_level: Compression level for the ``data`` column.
        max_rows_per_group: Override adaptive row group sizing.
        metadata: Additional key-value pairs for file-level metadata.
        base_resolve: File-level base resolution params.
    """

    # Adaptive row group sizing defaults.
    # See docs/parquet-layout.md for benchmarks and rationale.
    _TARGET_RG_DATA_BYTES: int = 10 * 1024 * 1024  # 10 MB of blob data per RG
    _MAX_RG_ROWS: int = 2000  # cap rows per RG even for tiny blobs

    def __init__(
        self,
        output: str | Path | Any | None = None,
        *,
        zarr_format: str = "3",
        data_compression: str = "none",
        data_compression_level: int | None = None,
        max_rows_per_group: int | None = None,
        metadata: dict[str, object] | None = None,
        base_resolve: dict | None = None,
    ) -> None:
        if output is None:
            self._output = None
        elif isinstance(output, (str, Path)):
            self._output = Path(output)
        else:
            self._output = output  # file-like object (BytesIO etc.)
        self._zarr_format = zarr_format
        self._data_compression = data_compression
        self._data_compression_level = data_compression_level
        self._max_rows_per_group = max_rows_per_group
        self._metadata = metadata or {}
        self._base_resolve = base_resolve

        # Buffered non-data rows (always in memory — they're small)
        self._non_data_rows: list[_Row] = []

        # Streaming state
        self._writer: pq.ParquetWriter | None = None
        self._schema: pa.Schema | None = None
        self._data_buf: list[_Row] = []  # current RG accumulator
        self._data_buf_bytes: int = 0
        self._closed = False

    def __enter__(self) -> Builder:
        return self

    def __exit__(self, *args: Any) -> None:
        if not self._closed:
            self.close()

    @staticmethod
    def _encode_metadata(metadata: dict[str, object] | None) -> str | None:
        if metadata is None:
            return None
        return rfc8785.dumps(metadata).decode("utf-8")

    @property
    def _streaming(self) -> bool:
        return self._output is not None

    def _ensure_writer(self) -> pq.ParquetWriter:
        """Lazily create the parquet writer on first data row."""
        if self._writer is not None:
            return self._writer

        file_meta = _make_file_meta(
            self._zarr_format, self._metadata, self._base_resolve,
        )
        self._schema = _SCHEMA.with_metadata(file_meta)
        self._writer = _make_writer(
            self._output, self._schema, self._data_compression_level,
        )
        return self._writer

    def _flush_data_buf(self) -> None:
        """Write accumulated data rows as one row group."""
        if not self._data_buf:
            return
        writer = self._ensure_writer()
        # Sort within the row group for optimal column statistics
        self._data_buf.sort(key=lambda r: r.path)
        table = _rows_to_table(self._data_buf, self._schema)
        writer.write_table(table)
        self._data_buf.clear()
        self._data_buf_bytes = 0

    def _stream_data_row(self, row: _Row) -> None:
        """Add a data row to the streaming buffer, flushing if needed."""
        self._data_buf.append(row)
        self._data_buf_bytes += row.blob_size

        target = self._max_rows_per_group or self._TARGET_RG_DATA_BYTES
        max_rows = self._max_rows_per_group or self._MAX_RG_ROWS

        if (self._data_buf_bytes >= self._TARGET_RG_DATA_BYTES
                or len(self._data_buf) >= max_rows):
            self._flush_data_buf()

    # -- Public API -----------------------------------------------------------

    def set_archive_metadata(
        self,
        metadata: dict[str, object],
        *,
        id: str | None = None,
    ) -> None:
        """Set metadata about the archive itself.

        This is metadata about the container (e.g. DICOM series UID,
        description, provenance) — not about any path within the archive.
        Stored on the ``""`` row.

        Calling this multiple times replaces the previous value.
        """
        self.set_path_metadata("", metadata, id=id)

    set_root_metadata = set_archive_metadata

    def set_path_metadata(
        self,
        path: str,
        metadata: dict[str, object],
        *,
        id: str | None = None,
    ) -> None:
        """Set metadata on a path (group or directory).

        Use :meth:`set_archive_metadata` for archive-level metadata.
        """
        path = _to_manifest_path(path)
        self._non_data_rows = [r for r in self._non_data_rows if r.path != path]
        self._non_data_rows.append(_Row(
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
        """Mount an external store at a path prefix."""
        if data is not None and data_z is not None:
            raise ValueError("Cannot set both data and data_z")
        path = _to_manifest_path(path)
        size = len(data) if data else len(data_z) if data_z else 0
        self._non_data_rows = [r for r in self._non_data_rows if r.path != path]
        self._non_data_rows.append(_Row(
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
        """Create a link entry that points to another path."""
        if path.endswith("/"):
            folder = True
        path = _to_manifest_path(path)
        target = _to_manifest_path(target)
        resolve_dict = {"path": {"target": target}}
        self._non_data_rows = [r for r in self._non_data_rows if r.path != path]
        self._non_data_rows.append(_Row(
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
        url: str | None = None,
        offset: int | None = None,
        length: int | None = None,
        resolve: dict | None = None,
        size: int | None = None,
        content_size: int | None = None,
        checksum: str | None = None,
        id: str | None = None,
        content_type: str | None = None,
        content_encoding: str | ContentEncoding | None = None,
        compress: str | ContentEncoding | None = None,
        source: str | None = None,
        base_resolve: dict | None = None,
        metadata: dict[str, object] | None = None,
        is_mount: bool = False,
        is_folder: bool = False,
    ) -> None:
        """Add an entry to the manifest.

        In streaming mode, data rows are written to disk immediately
        (no buffering). Non-data rows are always buffered.

        Args:
            path: Store path (e.g. ``"/zarr.json"``, ``"/arr/c/0"``).
            text: Inline text content.
            data: Inline binary content (uncompressed parquet column).
            data_z: Inline binary content (zstd parquet column).
            url: Shortcut for HTTP/file references. Builds the resolve
                dict automatically. Use with ``offset`` and ``length``
                for byte-range references::

                    builder.add("/c/0", url="https://example.com/data.bin",
                                offset=1024, length=4096)

                Equivalent to::

                    builder.add("/c/0", resolve={"http": {
                        "url": "https://example.com/data.bin",
                        "offset": 1024, "length": 4096}})

            offset: Byte offset within the URL resource (used with ``url``).
            length: Byte length within the URL resource (used with ``url``).
            resolve: Resolution dict keyed by scheme. Cannot be combined
                with ``url``.
            size: Logical (decompressed) size in bytes. Auto-computed
                from content if not provided.
            content_size: Size of the stored/compressed bytes (optional).
            checksum: Content hash. Auto-computed if not provided.
            id: Optional short identifier.
            content_type: MIME type (e.g. ``"application/json"``).
            content_encoding: Encoding of the data as stored. Set this
                when data is already compressed (e.g. from a zip file).
                Accepts :class:`ContentEncoding` or a string.
            compress: Compress the data on ingest. The uncompressed data
                is passed in ``data``, and the builder compresses it,
                stores the compressed bytes, and sets ``content_encoding``
                automatically. Accepts :class:`ContentEncoding` or a
                string (e.g. ``"deflate"``, ``"zstd"``).
                Cannot be used with ``content_encoding`` (data is already
                compressed) or ``data_z`` (parquet-level compression).
            source: Provenance string.
            base_resolve: Default resolution params for this entry.
            metadata: Per-entry metadata dict.
            is_mount: Mark as a mount point (addressing flag ``M``).
                Implies ``is_folder=True`` (mounts are always folders).
            is_folder: Mark as a folder/annotation row (addressing
                flag ``F``). Folder rows are excluded from zarr
                store listing.
        """
        if is_mount:
            is_folder = True

        # Build resolve dict from url shortcut
        if url is not None:
            if resolve is not None:
                raise ValueError("Cannot set both url and resolve")
            http_params: dict[str, Any] = {"url": url}
            if offset is not None:
                http_params["offset"] = offset
            if length is not None:
                http_params["length"] = length
            resolve = {"http": http_params}
            # Auto-set size from length if not provided
            if size is None and length is not None and content_encoding is None:
                size = length
        elif offset is not None or length is not None:
            raise ValueError("offset and length require url")

        if data is not None and data_z is not None:
            raise ValueError("Cannot set both data and data_z")
        if compress is not None and content_encoding is not None:
            raise ValueError(
                "Cannot set both compress and content_encoding. "
                "Use compress to compress on ingest, or content_encoding "
                "when data is already compressed."
            )
        if compress is not None and data_z is not None:
            raise ValueError("Cannot use compress with data_z")

        # Compress on ingest
        if compress is not None and data is not None:
            encoding = str(ContentEncoding(compress))
            uncompressed_size = len(data)
            checksum = checksum or git_blob_hash(data)
            data = _encode_content(data, encoding)
            content_encoding = encoding
            # size = logical (decompressed) size
            if size is None:
                size = uncompressed_size
            # content_size = compressed size on disk
            if content_size is None:
                content_size = len(data)

        # Normalize content_encoding to string
        if content_encoding is not None:
            content_encoding = str(ContentEncoding(content_encoding))

        path = _to_manifest_path(path)

        # Canonicalize JSON text
        if text is not None and (
            path.endswith(".json")
            or (content_type and "json" in content_type)
        ):
            try:
                text = rfc8785.dumps(json.loads(text)).decode("utf-8")
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass

        # Auto-compute size (logical size — decompressed if encoded)
        if size is None:
            if text is not None:
                size = len(text.encode("utf-8"))
            elif data is not None:
                size = len(data)
            elif data_z is not None:
                size = len(data_z)
            else:
                size = 0

        # Auto-compute checksum (of stored bytes, not decompressed)
        if checksum is None:
            if text is not None:
                checksum = git_blob_hash(text.encode("utf-8"))
            elif data is not None:
                checksum = git_blob_hash(data)
            elif data_z is not None:
                checksum = git_blob_hash(data_z)

        resolve_json = json.dumps(resolve, separators=(",", ":")) if resolve else None
        base_resolve_json = json.dumps(base_resolve, separators=(",", ":")) if base_resolve else None

        row = _Row(
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
            is_mount=is_mount,
            is_folder=is_folder,
        )

        if row.has_data and self._streaming:
            # Stream data rows directly to disk
            self._stream_data_row(row)
        else:
            # Buffer non-data rows (and data rows in batch mode)
            self._non_data_rows.append(row)

    def close(self) -> Path:
        """Finish writing the manifest.

        Flushes remaining data rows, writes non-data rows and the
        archive row, and closes the parquet writer.

        Returns:
            Path to the written file.
        """
        if self._closed:
            return self._output
        self._closed = True

        if not self._streaming:
            raise RuntimeError(
                "close() requires output path. Use write() for batch mode, "
                "or pass output= to Builder()."
            )

        # Flush any remaining data rows
        self._flush_data_buf()

        # Canonicalize JSON fields in non-data rows
        for r in self._non_data_rows:
            _canonicalize_row(r)

        # Separate non-data rows from archive row
        archive_rows = [r for r in self._non_data_rows if r.path == ""]
        other_rows = sorted(
            [r for r in self._non_data_rows if r.path != ""],
            key=lambda r: r.path,
        )

        # Ensure archive row exists
        if not archive_rows:
            archive_rows = [_Row(path="", size=0, is_folder=True)]

        writer = self._ensure_writer()

        # Write non-data rows as a single row group
        if other_rows:
            table = _rows_to_table(other_rows, self._schema)
            writer.write_table(table)

        # Write archive row as final row group
        table = _rows_to_table(archive_rows, self._schema)
        writer.write_table(table)

        writer.close()
        self._writer = None
        return self._output

    def write(self, output: str | Path | Any) -> Path | Any:
        """Batch mode: write all buffered rows to a file or file-like object.

        Args:
            output: File path (str/Path) or writable file-like object
                (e.g. ``io.BytesIO``). If a file-like object, it is
                written to but not closed.

        Returns:
            The ``output`` argument (Path if a path was given, or the
            file-like object).
        """
        is_path = isinstance(output, (str, Path))
        if is_path:
            output = Path(output)

        # Collect all rows (non-data buffer has everything in batch mode)
        all_rows = list(self._non_data_rows)

        # Canonicalize JSON fields
        for r in all_rows:
            _canonicalize_row(r)

        # Ensure archive row exists
        if not any(r.path == "" for r in all_rows):
            all_rows.append(_Row(path="", size=0, is_folder=True))

        # Separate into categories
        def _has_data(r: _Row) -> bool:
            return r.data is not None or r.data_z is not None

        data_rows = sorted(
            [r for r in all_rows if _has_data(r)], key=lambda r: r.path
        )
        non_data_rows = sorted(
            [r for r in all_rows if not _has_data(r) and r.path != ""],
            key=lambda r: r.path,
        )
        archive_rows = [r for r in all_rows if r.path == ""]
        rows = data_rows + non_data_rows + archive_rows

        # Build table
        file_meta = _make_file_meta(
            self._zarr_format, self._metadata, self._base_resolve,
        )
        schema = _SCHEMA.with_metadata(file_meta)
        table = _rows_to_table(rows, schema)

        writer = _make_writer(output, schema, self._data_compression_level)
        try:
            n_data = len(data_rows)
            n_non_data = len(non_data_rows)

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
                # Adaptive data row groups
                offset = 0
                target = self._TARGET_RG_DATA_BYTES
                max_rows = self._MAX_RG_ROWS
                rg_start = 0
                rg_data_bytes = 0
                rg_rows = 0

                for i in range(n_data):
                    rg_data_bytes += data_rows[i].blob_size
                    rg_rows += 1
                    if rg_data_bytes >= target or rg_rows >= max_rows:
                        writer.write_table(table.slice(rg_start, i - rg_start + 1))
                        rg_start = i + 1
                        rg_data_bytes = 0
                        rg_rows = 0

                if rg_start < n_data:
                    writer.write_table(table.slice(rg_start, n_data - rg_start))

                # Non-data rows: single row group
                if n_non_data > 0:
                    writer.write_table(table.slice(n_data, n_non_data))

                # Archive row: final row group
                writer.write_table(table.slice(n_data + n_non_data, len(archive_rows)))
        finally:
            writer.close()

        return output
