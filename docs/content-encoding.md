# Content Encoding (Transport-Level Decompression)

## Overview

The `content_encoding` column on a manifest entry tells the resolve
pipeline that the stored/fetched bytes are compressed and need
decompression before use. This is transparent to the consumer —
`resolve_entry()` returns decompressed bytes.

This is distinct from:
- **Parquet column compression** (`data_z`) — handled by parquet itself
- **Zarr codec pipelines** — handled by zarr after receiving bytes

`content_encoding` handles the case where data arrives already compressed
from an external source: a zip file entry, an HTTP response with
`Content-Encoding`, or a pre-compressed blob stored inline.

## Supported Encodings

| `content_encoding` | Module | Source | Notes |
|--------------------|--------|--------|-------|
| `deflate` | `zlib` (stdlib) | Zip default, HTTP | Raw deflate (no header) |
| `gzip` | `gzip` (stdlib) | HTTP, `.gz` files | Deflate + gzip header |
| `zlib` | `zlib` (stdlib) | General | Deflate + zlib header |
| `bz2` | `bz2` (stdlib) | Zip method 12, `.bz2` | |
| `lzma` | `lzma` (stdlib) | Zip method 14, `.xz` | |
| `zstd` | `zstandard` (pip) | Modern zip, HTTP | Optional dependency |
| `lz4` | `lz4` (pip) | Columnar formats | Optional dependency |
| `br` | `brotli` (pip) | HTTP (Brotli) | Optional dependency |

The first five use Python stdlib only. The last three require optional
pip packages and are silently unavailable if not installed.

## Usage

### Builder: storing pre-compressed data

```python
import zlib
from zmanifest import Builder

raw_pixels = b"\x00" * 65536
compressed = zlib.compress(raw_pixels)[2:-4]  # raw deflate

builder = Builder()
builder.add(
    "/vol/c/0",
    data=compressed,                # store compressed bytes
    content_encoding="deflate",     # tell resolver how to decompress
    size=len(raw_pixels),           # logical (decompressed) size
)
builder.write("output.zmp")
```

### Builder: referencing compressed data in a zip file

```python
builder = Builder()
builder.add(
    "/vol/c/0",
    resolve={"http": {"url": "data.zip", "offset": 1234, "length": 5678}},
    content_encoding="deflate",
    size=65536,
)
```

The resolver fetches 5678 bytes at offset 1234 from `data.zip`, then
decompresses them as raw deflate. The consumer receives 65536 bytes
of uncompressed data.

### Resolve: transparent decompression

```python
from zmanifest import Manifest
from zmanifest.resolve import resolve_entry

manifest = Manifest("output.zmp")
entry = manifest.get_entry("/vol/c/0")

# Returns decompressed bytes — encoding is handled internally
data = await resolve_entry(entry, manifest, resolvers)
```

## When to use `content_encoding` vs `data_z`

| Scenario | Use | Why |
|----------|-----|-----|
| Zarr chunks (pre-compressed by zarr codecs) | `data` column, no encoding | Already compressed; double-compression wastes CPU |
| Raw pixels stored compressed for size | `data` column + `content_encoding` | Compress once, decompress on read |
| Compressible metadata/text | `data_z` column | Let parquet handle it with ZSTD |
| Data fetched from a zip file | `resolve` + `content_encoding="deflate"` | Decompress the zip entry on fetch |
| Data fetched from HTTP with gzip | `resolve` + `content_encoding="gzip"` | Decompress the HTTP response body |
