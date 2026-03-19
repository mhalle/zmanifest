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


def git_blob_hash(content: bytes) -> str:
    """Compute git blob SHA-1: SHA-1('blob <size>\\0<content>')."""
    header = f"blob {len(content)}\0".encode()
    return hashlib.sha1(header + content).hexdigest()



def _parse_array_path_and_chunk_key(
    path: str, entry_type: str = "file"
) -> tuple[str | None, str | None]:
    """Extract array_path and chunk_key from a path with a ``c/`` separator.

    For ``temp/c/0/1``, returns ``("temp", "0/1")``.
    Returns ``(None, None)`` if no ``c/`` separator is found.
    """
    parts = path.split("/")
    # Find the chunk separator "c" in zarr v3 paths
    try:
        c_idx = parts.index("c")
        array_path = "/".join(parts[:c_idx]) or None
        chunk_key = "/".join(parts[c_idx + 1 :]) or None
        return array_path, chunk_key
    except ValueError:
        # v2 style or no separator — best effort
        return None, None


# ---------------------------------------------------------------------------
# Builder — row-level manifest builder
# ---------------------------------------------------------------------------


@dataclass
class _Row:
    path: str
    size: int
    id: str | None = None
    content_size: int | None = None  # logical/decoded size (optional)
    retrieval_key: str | None = None
    text: str | None = None
    data: bytes | None = None
    uri: str | None = None
    offset: int | None = None
    length: int | None = None
    array_path: str | None = None
    chunk_key: str | None = None
    media_type: str | None = None
    source: str | None = None
    checksum: str | None = None  # multihash, e.g. "sha256:abcdef..."
    base_uri: str | None = None
    metadata: str | None = None  # JSON string
    is_mount: bool = False
    is_link: bool = False

    @property
    def addressing(self) -> list[str]:
        flags = compute_addressing(
            text=self.text,
            data=self.data,
            retrieval_key=self.retrieval_key,
            uri=self.uri,
            is_link=self.is_link,
        )
        if self.is_mount:
            flags.insert(0, Addressing.MOUNT)
        return flags


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
        retrieval_scheme: str = "git-sha1",
        data_compression: str = "none",
        data_compression_level: int | None = None,
        max_rows_per_group: int | None = None,
        metadata: dict[str, object] | None = None,
    ) -> None:
        self._zarr_format = zarr_format
        self._retrieval_scheme = retrieval_scheme
        self._data_compression = data_compression
        self._data_compression_level = data_compression_level
        self._max_rows_per_group = max_rows_per_group
        self._metadata = metadata or {}
        self._rows: list[_Row] = []

    @staticmethod
    def _encode_metadata(metadata: dict[str, object] | None) -> str | None:
        if metadata is None:
            return None
        return json.dumps(metadata, separators=(",", ":"), sort_keys=True)

    def set_root_metadata(
        self,
        metadata: dict[str, object],
        *,
        id: str | None = None,
    ) -> None:
        """Set dataset-level metadata on the root entry.

        Stored as a row with path ``""`` and type ``"metadata"``.
        Calling this multiple times replaces the previous value.

        Args:
            metadata: Dataset-level metadata dict.
            id: Optional short identifier for this row.
        """
        self.set_path_metadata("", metadata, id=id)

    def set_path_metadata(
        self,
        path: str,
        metadata: dict[str, object],
        *,
        id: str | None = None,
    ) -> None:
        """Set metadata on a path (group or root) in the manifest.

        Use ``""`` for root, or a trailing-slash path for groups
        (e.g. ``"temperature/"``). These rows are invisible to the
        zarr Store interface but queryable via DuckDB.

        Calling this multiple times for the same path replaces the
        previous value.

        Args:
            path: Path for the metadata row. ``""`` for root, or
                ``"group/"`` for group-level metadata.
            metadata: Metadata dict (stored as JSON).
            id: Optional short identifier for this row.
        """
        # Normalize: ensure non-root paths end with /
        if path != "" and not path.endswith("/"):
            path = path + "/"
        # Remove any existing row for this path
        self._rows = [r for r in self._rows if r.path != path]
        self._rows.append(_Row(
            path=path,
            size=0,
            id=id,
            metadata=self._encode_metadata(metadata),
        ))

    def mount(
        self,
        path: str,
        uri: str,
        *,
        id: str | None = None,
        media_type: str | None = None,
        base_uri: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> None:
        """Mount an external store at a path prefix.

        The mounted store is opened lazily on first access and handles
        all reads under this prefix. Currently supports ``.zmp`` and
        ``.zarr.zip`` targets.

        Args:
            path: Mount point path (trailing ``/`` added if missing).
            uri: URI or local path to the mounted store.
            id: Optional short identifier.
            media_type: MIME type hint (auto-detected from extension if omitted).
            base_uri: Base URI for resolving relative URIs within the
                mounted store. Overrides the child's own base_uri.
            metadata: Per-entry metadata dict.
        """
        if not path.endswith("/"):
            path = path + "/"
        # Auto-detect media type from extension
        if media_type is None:
            if uri.endswith(".zmp"):
                media_type = "application/vnd.zmp"
            elif uri.endswith(".zarr.zip"):
                media_type = "application/zip"

        # Remove any existing row for this path
        self._rows = [r for r in self._rows if r.path != path]
        self._rows.append(_Row(
            path=path,
            size=0,
            id=id,
            uri=uri,
            media_type=media_type,
            base_uri=base_uri,
            metadata=self._encode_metadata(metadata),
            is_mount=True,
        ))

    def link(
        self,
        path: str,
        target: str,
        *,
        id: str | None = None,
        media_type: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> None:
        """Create a link entry that points to another path in the manifest.

        When resolved, the link's content comes from the target entry.
        The link row itself can carry its own metadata, id, and media_type.

        The target path is always relative to the manifest root.

        Args:
            path: The link's path in the manifest.
            target: Path of the target entry (relative to manifest root).
            id: Optional short identifier.
            media_type: MIME type hint.
            metadata: Per-entry metadata dict.
        """
        # Remove any existing row for this path
        self._rows = [r for r in self._rows if r.path != path]
        self._rows.append(_Row(
            path=path,
            size=0,
            id=id,
            uri=target,
            media_type=media_type,
            metadata=self._encode_metadata(metadata),
            is_link=True,
        ))

    def add(
        self,
        path: str,
        *,
        text: str | None = None,
        data: bytes | None = None,
        uri: str | None = None,
        offset: int | None = None,
        length: int | None = None,
        size: int | None = None,
        content_size: int | None = None,
        retrieval_key: str | None = None,
        id: str | None = None,
        array_path: str | None = None,
        chunk_key: str | None = None,
        media_type: str | None = None,
        source: str | None = None,
        checksum: str | None = None,
        base_uri: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> str | None:
        """Add an entry to the manifest.

        Supply one or more addressing sources. The ``addressing`` column
        is computed automatically from which fields are set. A
        ``retrieval_key`` is auto-computed (git-sha1) when ``text`` or
        ``data`` is provided and no explicit key is given.

        Args:
            path: Store path (e.g. ``"zarr.json"``, ``"arr/c/0/1"``).
            text: Inline text content.
            data: Inline binary content.
            uri: External URI or relative path.
            offset: Byte offset within the URI resource.
            length: Byte count to read from the URI resource.
            size: Size in bytes of the stored content. Auto-computed
                from ``text``, ``data``, or ``length`` if not provided.
            content_size: Logical/decoded size in bytes (optional).
            retrieval_key: Content hash. Auto-computed from ``text``
                (canonical JSON) or ``data`` (raw bytes) if not provided.
            id: Optional short identifier for cross-referencing.
            array_path: Zarr array this entry belongs to.
            chunk_key: Chunk coordinates within the array.
            media_type: MIME type of the content.
            source: Provenance string.
            checksum: Multihash for verification (e.g. ``"sha256:abc..."``).
            base_uri: Base URI for resolving this entry's relative
                uri. Overrides the store-level base_uri.
            metadata: Per-entry metadata dict (stored as JSON).

        Returns:
            The retrieval key if one was computed or provided, else None.
        """
        # Auto-compute size
        if size is None:
            if text is not None:
                size = len(text.encode("utf-8"))
            elif data is not None:
                size = len(data)
            elif length is not None:
                size = length
            else:
                size = 0

        # Auto-compute retrieval_key
        if retrieval_key is None:
            if text is not None:
                canonical = rfc8785.dumps(json.loads(text))
                retrieval_key = git_blob_hash(canonical)
            elif data is not None:
                retrieval_key = git_blob_hash(data)

        # Auto-infer array_path / chunk_key from path
        if array_path is None and chunk_key is None:
            array_path, chunk_key = _parse_array_path_and_chunk_key(path)

        self._rows.append(_Row(
            path=path,
            size=size,
            id=id,
            content_size=content_size,
            retrieval_key=retrieval_key,
            text=text,
            data=data,
            uri=uri,
            offset=offset,
            length=length,
            array_path=array_path,
            chunk_key=chunk_key,
            media_type=media_type,
            source=source,
            checksum=checksum,
            base_uri=base_uri,
            metadata=self._encode_metadata(metadata),
        ))
        return retrieval_key

    def write(self, output: str | Path) -> Path:
        """Write the manifest to a ZMP parquet file.

        Returns:
            Path to the written file.
        """
        output = Path(output)

        # Sort rows by path
        rows = sorted(self._rows, key=lambda r: r.path)

        # Build columns
        def _col(attr: str) -> list[Any]:
            return [getattr(r, attr) for r in rows]

        table = pa.table(
            {
                "path": pa.array(_col("path"), type=pa.string()),
                "id": pa.array(_col("id"), type=pa.string()),
                "size": pa.array(_col("size"), type=pa.int64()),
                "content_size": pa.array(_col("content_size"), type=pa.int64()),
                "retrieval_key": pa.array(_col("retrieval_key"), type=pa.string()),
                "text": pa.array(_col("text"), type=pa.string()),
                "data": pa.array(_col("data"), type=pa.binary()),
                "uri": pa.array(_col("uri"), type=pa.string()),
                "offset": pa.array(_col("offset"), type=pa.int64()),
                "length": pa.array(_col("length"), type=pa.int64()),
                "array_path": pa.array(_col("array_path"), type=pa.string()),
                "chunk_key": pa.array(_col("chunk_key"), type=pa.string()),
                "media_type": pa.array(_col("media_type"), type=pa.string()),
                "source": pa.array(_col("source"), type=pa.string()),
                "checksum": pa.array(_col("checksum"), type=pa.string()),
                "base_uri": pa.array(_col("base_uri"), type=pa.string()),
                "metadata": pa.array(_col("metadata"), type=pa.string()),
                "addressing": pa.array(
                    [r.addressing for r in rows], type=pa.list_(pa.string())
                ),
            }
        )

        # File-level metadata
        file_meta: dict[bytes, bytes] = {
            b"zmp_version": json.dumps("0.1.0").encode(),
            b"zarr_format": json.dumps(self._zarr_format).encode(),
            b"retrieval_scheme": json.dumps(self._retrieval_scheme).encode(),
        }
        for k, v in self._metadata.items():
            file_meta[k.encode()] = v.encode() if isinstance(v, str) else json.dumps(v).encode()

        schema = table.schema.with_metadata(file_meta)
        table = table.cast(schema)

        # Compression
        compression = {col: "zstd" for col in table.schema.names}
        compression["data"] = self._data_compression
        use_dictionary = {col: True for col in table.schema.names}
        use_dictionary["data"] = False

        # Row group sizing
        if self._max_rows_per_group is not None:
            max_rg = self._max_rows_per_group
        else:
            has_inline_data = any(r.data is not None for r in rows)
            if has_inline_data:
                max_rg = 2
            else:
                max_rg = max(1, math.ceil(len(table) / 16))

        compression_level = None
        if self._data_compression_level is not None:
            compression_level = {"data": self._data_compression_level}

        writer = pq.ParquetWriter(
            str(output),
            schema,
            compression=compression,
            compression_level=compression_level,
            use_dictionary=use_dictionary,
        )
        try:
            n = len(table)
            i = 0
            while i < n:
                end = min(i + max_rg, n)
                writer.write_table(table.slice(i, end - i))
                i = end
        finally:
            writer.close()

        return output
