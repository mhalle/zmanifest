# zmanifest

A content-addressed file manifest format backed by Apache Parquet.

ZManifest describes a collection of files — metadata, binary blobs, virtual references — in a single `.zmp` Parquet file. Content can be inlined for self-contained archives or resolved from external sources (HTTP, git, local files, zip archives).

For Zarr v3 store integration, see [zarr-zmp](https://github.com/mhalle/zarr-zmp).

## Install

```bash
pip install zmanifest
```

## CLI

The `zmp` command-line tool provides archive-style access to `.zmp` files:

```bash
# Inspect
zmp info archive.zmp
zmp list -l archive.zmp
zmp cat archive.zmp /zarr.json
zmp metadata archive.zmp

# Extract
zmp extract archive.zmp -o ./output/
zmp get archive.zmp /arr/c/0 -o chunk.bin

# Create
zmp create output.zmp ./data/ --base ./data/

# Import from zip (local or remote URL)
zmp import-zip data.zarr.zip output.zmp
zmp import-zip --virtual https://example.com/data.zip output.zmp

# Verify
zmp validate archive.zmp

# Convert
zmp hash input.zmp output.zmp
zmp dehydrate input.zmp refs.zmp --chunk-dir ./blobs/
zmp hydrate refs.zmp full.zmp --chunk-dir ./blobs/
```

### `zmp info`

```
$ zmp info dataset.zmp
File:         dataset.zmp
Version:      0.15.0
Zarr format:  3
Entries:      142 (3 text, 100 data, 38 ref, 1 folder)
Total size:   12.4 MB
Archive metadata:
  description: CT scan
  series_uid: 1.2.840.113619.1234
```

### `zmp list`

```
$ zmp list -l dataset.zmp
D       4096  a1b2c3d4e5f6  /arr/c/0
D       4096  f6e5d4c3b2a1  /arr/c/1
R      65536                /arr/c/2
T        245  74d12a43f68d  /zarr.json
```

### `zmp import-zip --virtual`

Imports a zip file as virtual references — the data stays in the zip,
accessed by byte offset via HTTP range requests:

```
$ zmp import-zip --virtual https://example.com/data.zarr.zip output.zmp
Imported 2455 entries from https://... (virtual) to output.zmp
```

Works with remote URLs (uses `remotezip` — only fetches the central
directory, not the whole file).

## Python API

### Build a manifest

```python
from zmanifest import Builder, ContentEncoding

builder = Builder()

# Archive-level metadata
builder.set_archive_metadata({"description": "CT scan", "modality": "CT"})

# Inline text (JSON metadata)
builder.add("/zarr.json", text='{"zarr_format":3,"node_type":"group"}')

# Inline binary data
builder.add("/volume/c/0", data=chunk_bytes)

# Compress on ingest
builder.add("/volume/c/1", data=raw_pixels, compress=ContentEncoding.ZSTD)

# Virtual reference (URL shortcut)
builder.add("/volume/c/2",
    url="https://example.com/data.bin",
    offset=3100,
    length=32768,
)

# Pre-compressed data (from a zip file)
builder.add("/volume/c/3",
    data=deflated_bytes,
    content_encoding="deflate",
    size=65536,  # decompressed size
)

# Mount external manifests
builder.mount("scans/ct", resolve={"http": {"url": "ct_scan.zmp"}})

builder.write("output.zmp")
```

### Streaming mode (large archives)

```python
# Data rows stream to disk — no buffering in memory
with Builder(output="large.zmp") as builder:
    builder.add("/zarr.json", text=metadata_json)
    for i, chunk in enumerate(chunks):
        builder.add(f"/arr/c/{i}", data=chunk)
```

### Read a manifest

```python
from zmanifest import Manifest

m = Manifest("dataset.zmp")

# Dict-like access
entry = m["/zarr.json"]
"/zarr.json" in m  # True

# Archive and path metadata
m.archive_metadata              # {"description": "CT scan", ...}
m.path_metadata("/volume")      # {"voxel_size": [0.5, 0.5, 1.0]}

# Per-entry metadata
m.get_metadata(path="/volume/c/0")
m.get_metadata(id="slice_42")

# Listing
list(m.list_paths())             # all paths
list(m.list_prefix("/volume/c")) # filtered
list(m.list_dir("/"))            # top-level directory
```

### Resolve content

```python
import asyncio
from zmanifest import Manifest, resolve_entry
from zmanifest.resolver import HttpResolver

m = Manifest("dataset.zmp")
resolvers = {"http": HttpResolver()}

entry = m["/volume/c/0"]
data = asyncio.run(resolve_entry(entry, m, resolvers))
```

Content encoding is handled transparently — if an entry has
`content_encoding="deflate"`, the resolver decompresses automatically.

### Convert between manifest variants

```python
from zmanifest import dehydrate, hydrate, hash

# Compute git-sha1 checksums
hash("no_keys.zmp", "with_keys.zmp")

# Strip inline data, write blobs to directory
dehydrate("full.zmp", "refs.zmp", chunk_dir="./blobs")

# Resolve references, inline everything
hydrate("refs.zmp", "full.zmp", resolver, prefix="/temperature/")
```

### Query with DuckDB

```sql
-- All entries with inline data
SELECT path, size, checksum
FROM read_parquet('dataset.zmp')
WHERE addressing LIKE '%D%';

-- Per-entry metadata
SELECT path, json_extract(metadata, '$.SliceLocation') as z
FROM read_parquet('dataset.zmp')
WHERE path != ''
ORDER BY z;

-- Archive metadata
SELECT json_extract(metadata, '$.modality')
FROM read_parquet('dataset.zmp')
WHERE path = '';
```

## Column schema

| Column | Type | Description |
|--------|------|-------------|
| `path` | `string` | Absolute path (`/arr/c/0`). `""` = archive row |
| `id` | `string` | Optional short identifier |
| `size` | `int64` | Logical (decompressed) size in bytes |
| `content_size` | `int64` | Stored (compressed) size in bytes |
| `checksum` | `string` | Content hash (git-sha1) |
| `text` | `string` | Inline text content |
| `data` | `binary` | Inline binary (parquet compression disabled) |
| `data_z` | `binary` | Inline binary (parquet zstd compression) |
| `resolve` | `string` | Resolution dict as JSON |
| `base_resolve` | `string` | Base resolution params as JSON |
| `content_type` | `string` | MIME type |
| `content_encoding` | `string` | Transport compression (`deflate`, `zstd`, etc.) |
| `source` | `string` | Provenance string |
| `metadata` | `string` | Per-entry JSON metadata |
| `addressing` | `string` | Resolution flags: `T`ext, `D`ata, `Z` (data_z), `R`esolve, `L`ink, `M`ount, `F`older |

## Content addressing

Every entry with inline content gets a `checksum` — the git blob SHA-1
(`SHA-1("blob <size>\0<content>")`). JSON text is canonicalized via
[RFC 8785](https://datatracker.ietf.org/doc/rfc8785/) before hashing.

When using `compress=`, the checksum is of the data before compression.
When using `content_encoding=` with pre-compressed data, the checksum
is of the bytes as stored.

## Development

```bash
uv sync --extra test
uv run pytest tests/ -v
```

## License

BSD-3-Clause
