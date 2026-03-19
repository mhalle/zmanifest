# zmanifest

A content-addressed file manifest format backed by Apache Parquet.

ZManifest describes a collection of files — metadata, binary blobs, virtual references — in a single Parquet file. Each row represents an entry. Content can be inlined for self-contained archives or resolved from external content-addressed blob stores (git, HTTP, local files).

The format is framework-agnostic. For Zarr v3 store integration, see [zarr-zmp](https://github.com/mhalle/zarr-zmp).

## Features

- **Self-contained mode**: all content in one `.zmp` parquet file
- **External blob mode**: blobs stored separately by content hash, manifest as lightweight index
- **Git integration**: resolve blobs from bare git repos via [vost](https://pypi.org/project/vost/) (dulwich), with deduplication across datasets
- **Content-addressed**: every entry gets a git-sha1 `retrieval_key`, JSON canonicalized via RFC 8785
- **Queryable with DuckDB/Spark/Polars**: standard parquet — query your manifest with SQL or dataframes
- **Per-entry metadata**: JSON metadata column on every row, queryable with `json_extract`
- **Path annotations**: attach metadata to groups and the dataset root
- **Addressing flags**: `list<string>` column (`T`ext, `D`ata, `K`ey, `U`RI, `M`ount) for fast filtering without decompressing content columns
- **Manifest conversion**: hydrate, dehydrate, and hash operations to transform between self-contained and reference-only manifests
- **Virtual references**: byte-range references into external files (DICOM, NetCDF, HDF5, etc.)
- **Relative URI resolution**: `external_uri` values resolve against per-row `base_uri`, file-level metadata, or the manifest's own location
- **Mount support**: compose manifests by mounting child manifests at path prefixes
- **Adaptive row group sizing**: optimized for both lazy access and analytics queries

## Install

```bash
pip install zmanifest

# Optional extras
pip install zmanifest[git]   # Git/vost resolver
pip install zmanifest[http]  # HTTP resolver (httpx)
```

## Quick start

### Build a manifest

```python
from zmanifest import Builder

builder = Builder()

# Dataset-level metadata (stored at path "")
builder.set_root_metadata({"description": "CT scan", "modality": "CT"})

# Group-level metadata (stored at path "group/")
builder.set_path_metadata("volume", {"voxel_size": [0.5, 0.5, 1.0]})

# Inline text (JSON metadata, sidecars, etc.)
builder.add("zarr.json", text='{"zarr_format":3,"node_type":"group"}')

# Inline binary data
builder.add("volume/c/0/0/0", data=chunk_bytes)

# Virtual reference into an external file
builder.add("volume/c/1/0/0",
    uri="s3://bucket/file.dcm",
    offset=3100,
    length=32768,
    metadata={"SliceLocation": 42.5, "InstanceNumber": 1},
)

# Reference by content hash (resolved via blob store)
builder.add("volume/c/2/0/0",
    retrieval_key="e5f6a1b2...",
    size=32768,
)

# Multiple addressing paths (inline + external fallback)
builder.add("volume/c/3/0/0",
    data=chunk_bytes,
    uri="https://cdn.example.com/backup",
    offset=0,
    length=32768,
)

# Mount external manifests at a path prefix
builder.mount("scans/ct", "s3://bucket/ct_scan.zmp")
builder.mount("scans/mri", "/data/mri.zmp",
    base_uri="https://cdn.example.com/mri/")  # override blob resolution for child

builder.write("output.zmp")
```

### Read a manifest

```python
from zmanifest import Manifest

m = Manifest("dataset.zmp")

# Dict-like access by path
entry = m["zarr.json"]
"zarr.json" in m  # True

# Access by id
entry = m.by_id["slice_42"]

# Root and path metadata
m.root_metadata                      # {"description": "CT scan", ...}
m.path_metadata("volume")            # {"voxel_size": [0.5, 0.5, 1.0]}

# Per-entry metadata
m.get_metadata(path="volume/c/0/0/0")
m.get_metadata(id="slice_42")

# Addressing — how content can be resolved
from zmanifest import Addressing

entry = m["zarr.json"]
entry.addressing                       # [Addressing.TEXT, Addressing.KEY]
Addressing.TEXT in entry.addressing    # True
Addressing.DATA in entry.addressing   # False
```

### Resolve content

```python
import asyncio
from zmanifest import Manifest, TemplateResolver, resolve_entry

m = Manifest("dataset.zmp")
resolver = TemplateResolver("./blobs/{hash}")

entry = m["volume/c/0/0/0"]
data = asyncio.run(resolve_entry(entry, m, resolver))
```

### Convert between manifest variants

```python
from zmanifest import dehydrate, hydrate, hash, TemplateResolver

# Compute git-sha1 hashes for entries missing them
hash("no_keys.zmp", "with_keys.zmp")

# Strip inline data, write blobs to directory
dehydrate("full.zmp", "refs.zmp", chunk_dir="./blobs")

# Resolve all references, inline everything
resolver = TemplateResolver("./blobs/{hash}")
hydrate("refs.zmp", "full.zmp", resolver)

# Partial hydrate — only specific paths
hydrate("refs.zmp", "partial.zmp", resolver, prefix="temperature/")
hydrate("refs.zmp", "partial.zmp", resolver, paths=["arr/c/0", "arr/c/1"])
```

### Query with DuckDB

```sql
-- Find entries for a specific array
SELECT path, retrieval_key, size
FROM read_parquet('dataset.zmp')
WHERE array_path = 'temperature';

-- Query per-entry metadata
SELECT path, json_extract(metadata, '$.SliceLocation') as z
FROM read_parquet('dataset.zmp')
WHERE path NOT LIKE '%/'
ORDER BY z;

-- Filter by addressing mode
SELECT * FROM read_parquet('dataset.zmp')
WHERE list_contains(addressing, 'U');  -- all URI-referenced entries

-- Root metadata
SELECT json_extract(metadata, '$.modality')
FROM read_parquet('dataset.zmp')
WHERE path = '';
```

### Query with Polars

```python
import polars as pl

df = pl.read_parquet("dataset.zmp")

# Virtual references
df.filter(pl.col("addressing").list.contains("U"))

# Entries with inline data
df.filter(pl.col("addressing").list.contains("D"))
```

## Blob resolver templates

The `TemplateResolver` supports `{hash}` templates with slice syntax:

```python
from zmanifest import TemplateResolver

# Flat layout
TemplateResolver("/data/blobs/{hash}")

# Git fanout (objects/ab/cdef...)
TemplateResolver("https://cdn.com/{hash[:2]}/{hash[2:]}")
```

The `GitResolver` reads from bare git repos:

```python
from zmanifest import GitResolver

resolver = GitResolver("/data/repo.git")
```

## URI resolution

Relative `external_uri` values (no scheme, no leading `/`) are resolved against a base URI. The precedence is:

1. **Per-row `base_uri` column** — set on individual entries or mount points
2. **File-level `base_uri` metadata** — in the parquet key-value pairs
3. **Manifest location** — parent directory of the manifest URL or path

```python
# Manifest at https://cdn.com/datasets/climate/manifest.zmp
# Entry with external_uri "chunks/abc123"
# Resolves to: https://cdn.com/datasets/climate/chunks/abc123
```

## Column schema

| Column | Type | Description |
|--------|------|-------------|
| `path` | `string` | Entry key. `""` = root annotation, `"group/"` = group annotation |
| `id` | `string` | Optional short identifier for cross-referencing rows |
| `size` | `int64` | Size in bytes of the stored content |
| `content_size` | `int64` | Logical/decoded size in bytes (optional) |
| `retrieval_key` | `string` | Content hash for blob resolution |
| `text` | `string` | Inline text content |
| `data` | `binary` | Inline binary content (parquet compression disabled) |
| `external_uri` | `string` | External URI or relative path |
| `offset` | `int64` | Byte offset within external resource |
| `length` | `int64` | Byte count to read from external resource |
| `array_path` | `string` | Array/group this entry belongs to |
| `chunk_key` | `string` | Chunk coordinates within the array |
| `media_type` | `string` | MIME type |
| `source` | `string` | Provenance string |
| `checksum` | `string` | Multihash for verification (e.g. `"sha256:abc..."`) |
| `base_uri` | `string` | Base URI for resolving this entry's relative URIs |
| `metadata` | `string` | Per-entry JSON metadata |
| `addressing` | `list<string>` | Resolution flags: `T`ext, `D`ata, `K`ey, `U`RI, `M`ount |

## Content addressing

Every entry with inline content gets a `retrieval_key` — the git blob SHA-1 of its content (`SHA-1("blob <size>\0<content>")`). JSON text is canonicalized via [RFC 8785](https://datatracker.ietf.org/doc/rfc8785/) before hashing for deterministic keys regardless of serialization order.

This means:
- Identical content across datasets shares the same hash and is stored once
- A manifest can be "promoted" from self-contained to external by dehydrating — the hashes are already correct
- Manifests can be committed into git repos alongside their data for versioned snapshots

## Development

```bash
uv sync --extra test
uv run pytest tests/ -v
```

## License

Apache-2.0
