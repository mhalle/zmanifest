"""Convert between ZMP manifest variants.

- **hash**: compute git-sha1 retrieval keys for entries missing them
- **hydrate**: resolve external references and inline the data (full or partial)
- **dehydrate**: strip inline data, keep retrieval keys, optionally write blobs out
"""

from __future__ import annotations

import asyncio
import json
import math
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import rfc8785

from .builder import git_blob_hash
from .manifest import Manifest
from .resolver import BlobResolver


def hash(
    input: str | Path,
    output: str | Path,
    *,
    resolver: BlobResolver | None = None,
    max_rows_per_group: int | None = None,
) -> Path:
    """Compute git-sha1 retrieval keys for manifest entries.

    For entries with inline content (``text`` or ``data``), the hash is
    computed directly. JSON text is canonicalized via RFC 8785 before
    hashing.

    For entries without inline content, a ``resolver`` can be provided to
    fetch the bytes and compute the hash. Entries that can't be resolved
    are left with their existing retrieval key (or null).

    Args:
        input: Path to the source ``.zmp`` file.
        output: Path for the output ``.zmp`` file.
        resolver: Optional resolver for entries without inline content.
        max_rows_per_group: Override row group sizing.

    Returns:
        Path to the written file.
    """
    output = Path(output)
    pf = pq.ParquetFile(str(input))
    table = pf.read()

    path_col = table.column("path")
    text_col = table.column("text") if "text" in table.column_names else None
    data_col = table.column("data") if "data" in table.column_names else None
    rk_col = table.column("retrieval_key") if "retrieval_key" in table.column_names else None

    new_keys: list[str | None] = []

    for i in range(len(table)):
        existing_key = rk_col[i].as_py() if rk_col is not None else None

        # Try inline text (canonicalize JSON)
        text = text_col[i].as_py() if text_col is not None else None
        if text is not None:
            canonical = rfc8785.dumps(json.loads(text))
            new_keys.append(git_blob_hash(canonical))
            continue

        # Try inline data
        data = data_col[i].as_py() if data_col is not None else None
        if data is not None:
            new_keys.append(git_blob_hash(data))
            continue

        # Try resolver
        if existing_key is None and resolver is not None:
            # Can't resolve without a key — skip
            new_keys.append(None)
            continue

        if existing_key is not None and resolver is not None:
            # Already have a key, keep it
            new_keys.append(existing_key)
            continue

        # Keep whatever we have
        new_keys.append(existing_key)

    # Replace retrieval_key column
    new_rk = pa.array(new_keys, type=pa.string())
    if "retrieval_key" in table.column_names:
        idx = table.schema.get_field_index("retrieval_key")
        table = table.set_column(idx, table.schema.field(idx), new_rk)
    else:
        table = table.append_column("retrieval_key", new_rk)

    if max_rows_per_group is None:
        has_data = any(
            (data_col[i].as_py() if data_col is not None else None) is not None
            for i in range(len(table))
        )
        if has_data:
            max_rows_per_group = 2
        else:
            max_rows_per_group = max(1, math.ceil(len(table) / 16))

    _write_table(table, output, max_rows_per_group)
    return output


def dehydrate(
    input: str | Path,
    output: str | Path,
    *,
    chunk_dir: str | Path | None = None,
    max_rows_per_group: int | None = None,
) -> Path:
    """Strip inline data from a ZMP manifest, keeping retrieval keys.

    Produces a lightweight reference-only manifest. If ``chunk_dir`` is
    provided, inline chunk data is written to that directory as files
    named by their retrieval key (deduped).

    Metadata (``text`` column) is always preserved inline.

    Args:
        input: Path to the source ``.zmp`` file.
        output: Path for the output ``.zmp`` file.
        chunk_dir: If set, write chunk blobs here before stripping.
        max_rows_per_group: Override adaptive row group sizing.

    Returns:
        Path to the written file.
    """
    output = Path(output)

    if chunk_dir is not None:
        chunk_dir = Path(chunk_dir)
        chunk_dir.mkdir(parents=True, exist_ok=True)

    # Read full table including data
    pf = pq.ParquetFile(str(input))
    table = pf.read()

    # Write out chunk blobs if requested
    if chunk_dir is not None and "data" in table.column_names:
        data_col = table.column("data")
        rk_col = table.column("retrieval_key") if "retrieval_key" in table.column_names else None
        for i in range(len(table)):
            blob = data_col[i].as_py()
            if blob is None:
                continue
            key = rk_col[i].as_py() if rk_col is not None else git_blob_hash(blob)
            if key is None:
                key = git_blob_hash(blob)
            blob_path = chunk_dir / key
            if not blob_path.exists():
                blob_path.write_bytes(blob)

    # Null out the data column
    null_data = pa.array([None] * len(table), type=pa.binary())
    idx = table.schema.get_field_index("data")
    table = table.set_column(idx, table.schema.field(idx), null_data)

    # Write with appropriate row group sizing
    if max_rows_per_group is None:
        max_rows_per_group = max(1, math.ceil(len(table) / 16))

    _write_table(table, output, max_rows_per_group)
    return output


def hydrate(
    input: str | Path,
    output: str | Path,
    resolver: BlobResolver,
    *,
    paths: list[str] | None = None,
    prefix: str | None = None,
    max_rows_per_group: int | None = None,
) -> Path:
    """Resolve external references and inline data.

    Without filters, resolves all entries (full hydrate). With ``paths``
    or ``prefix``, only resolves matching entries (partial hydrate).

    Args:
        input: Path to the source ``.zmp`` file.
        output: Path for the output ``.zmp`` file.
        resolver: Blob resolver to fetch content by retrieval key.
        paths: Only hydrate these specific paths.
        prefix: Only hydrate paths starting with this prefix.
        max_rows_per_group: Override row group sizing (default: 2).

    Returns:
        Path to the written file.
    """
    output = Path(output)
    pf = pq.ParquetFile(str(input))
    table = pf.read()

    path_set = set(paths) if paths else None
    filter_active = path_set is not None or prefix is not None

    data_list: list[bytes | None] = []
    data_col = table.column("data") if "data" in table.column_names else None
    rk_col = table.column("retrieval_key") if "retrieval_key" in table.column_names else None
    path_col = table.column("path")
    text_col = table.column("text") if "text" in table.column_names else None

    for i in range(len(table)):
        # Already has inline data — keep it
        existing = data_col[i].as_py() if data_col is not None else None
        if existing is not None:
            data_list.append(existing)
            continue

        # Already has inline text — nothing to hydrate
        text = text_col[i].as_py() if text_col is not None else None
        if text is not None:
            data_list.append(None)
            continue

        # Check filter
        if filter_active:
            p = path_col[i].as_py()
            match = (path_set is not None and p in path_set) or (
                prefix is not None and p.startswith(prefix)
            )
            if not match:
                data_list.append(None)
                continue

        # Resolve via retrieval key
        key = rk_col[i].as_py() if rk_col is not None else None
        if key is not None:
            blob = asyncio.run(resolver.resolve(key))
            data_list.append(blob)
        else:
            data_list.append(None)

    # Replace data column
    new_data = pa.array(data_list, type=pa.binary())
    idx = table.schema.get_field_index("data")
    table = table.set_column(idx, table.schema.field(idx), new_data)

    if max_rows_per_group is None:
        has_data = any(v is not None for v in data_list)
        if has_data:
            max_rows_per_group = 2
        else:
            max_rows_per_group = max(1, math.ceil(len(table) / 16))

    _write_table(table, output, max_rows_per_group)
    return output


def _write_table(table: pa.Table, output: Path, max_rows_per_group: int) -> None:
    """Write a table with ZMP compression conventions."""
    compression = {col: "zstd" for col in table.schema.names}
    if "data" in table.schema.names:
        compression["data"] = "none"
    use_dictionary = {col: True for col in table.schema.names}
    if "data" in table.schema.names:
        use_dictionary["data"] = False

    writer = pq.ParquetWriter(
        str(output),
        table.schema,
        compression=compression,
        use_dictionary=use_dictionary,
    )
    try:
        n = len(table)
        i = 0
        while i < n:
            end = min(i + max_rows_per_group, n)
            writer.write_table(table.slice(i, end - i))
            i = end
    finally:
        writer.close()
