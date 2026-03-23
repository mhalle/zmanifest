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
| `zstd` | `zstandard` | Modern zip, HTTP | |
| `lz4` | `lz4` | Columnar formats | |
| `br` | `brotli` | HTTP (Brotli) | |

All are required dependencies.

The `ContentEncoding` enum provides these as typed values:

```python
from zmanifest import ContentEncoding

ContentEncoding.DEFLATE  # "deflate"
ContentEncoding.GZIP     # "gzip"
ContentEncoding.ZSTD     # "zstd"
# etc.
```

## Size, content_size, and checksum

Three fields interact with `content_encoding`. Their meaning depends
on how the entry was created:

### `Builder.add(data=X, compress="deflate")`

The builder has the uncompressed data and compresses it.

| Field | Value | Meaning |
|-------|-------|---------|
| `size` | `len(X)` | Logical size — decompressed bytes the consumer receives |
| `content_size` | `len(compressed)` | Stored size — compressed bytes on disk |
| `checksum` | `git_blob_hash(X)` | Hash of data before compression |
| `content_encoding` | `"deflate"` | How to decompress |

The checksum identifies the *logical content*. Two entries with the
same checksum produce the same bytes after decompression, regardless
of encoding.

### `Builder.add(data=compressed_bytes, content_encoding="deflate", size=65536)`

The caller provides pre-compressed data (e.g. extracted from a zip file).
The builder doesn't know what the decompressed bytes look like.

| Field | Value | Meaning |
|-------|-------|---------|
| `size` | `65536` (caller provides) | Logical size — caller must know this |
| `content_size` | None (or `len(compressed_bytes)`) | Stored size |
| `checksum` | `git_blob_hash(compressed_bytes)` | Hash of bytes as stored |
| `content_encoding` | `"deflate"` | How to decompress |

The checksum identifies the *stored representation*. This is the only
option when the builder never sees the uncompressed bytes.

### `Builder.add(data=X)` (no encoding)

No compression. Everything is straightforward.

| Field | Value | Meaning |
|-------|-------|---------|
| `size` | `len(X)` | Logical = stored (same thing) |
| `content_size` | None | Not needed |
| `checksum` | `git_blob_hash(X)` | Hash of bytes |
| `content_encoding` | None | No decompression |

### Summary

| Scenario | `size` | `content_size` | `checksum` of |
|----------|--------|----------------|---------------|
| `compress=` | decompressed | compressed | decompressed (before our compression) |
| `content_encoding=` | caller sets | compressed | compressed (as provided to us) |
| no encoding | byte count | None | bytes as stored |

## Usage

### Compress on ingest

`Builder.add(compress=...)` compresses data as it's added:

```python
from zmanifest import Builder, ContentEncoding

builder = Builder()

# String form
builder.add("/vol/c/0", data=raw_pixels, compress="deflate")

# Enum form
builder.add("/vol/c/1", data=raw_pixels, compress=ContentEncoding.ZSTD)

# Cannot combine with content_encoding (data is already compressed)
# or data_z (parquet-level compression)
```

### Storing pre-compressed data

When data is already compressed (from a zip file, a compressed blob
store, etc.), pass it as-is and declare the encoding:

```python
import zlib

compressed = zlib.compress(raw_pixels)[2:-4]  # raw deflate

builder = Builder()
builder.add(
    "/vol/c/0",
    data=compressed,                # store compressed bytes as-is
    content_encoding="deflate",     # tell resolver how to decompress
    size=len(raw_pixels),           # caller must provide logical size
)
```

### Referencing compressed data at a remote location

```python
builder = Builder()
builder.add(
    "/vol/c/0",
    resolve={"http": {"url": "data.zip", "offset": 1234, "length": 5678}},
    content_encoding="deflate",
    size=65536,                     # logical (decompressed) size
)
```

The resolver fetches 5678 bytes at offset 1234, then decompresses
as raw deflate. The consumer receives 65536 bytes.

### Resolve: transparent decompression

```python
from zmanifest import Manifest
from zmanifest.resolve import resolve_entry

manifest = Manifest("output.zmp")
entry = manifest.get_entry("/vol/c/0")

# Returns decompressed bytes — encoding is handled internally
data = await resolve_entry(entry, manifest, resolvers)
```

## When to use what

| Scenario | Approach |
|----------|----------|
| Zarr chunks (pre-compressed by zarr codecs) | `data=`, no encoding — already compressed, don't double-compress |
| Raw pixels, want smaller archive | `data=, compress="zstd"` — builder compresses for you |
| Data from a zip file | `data=, content_encoding="deflate"` — store as-is, decompress on read |
| Reference to compressed remote data | `resolve=, content_encoding="deflate"` — fetch and decompress |
| Compressible metadata/text | `data_z=` — let parquet's ZSTD column compression handle it |
| Data fetched from HTTP with gzip | `resolve=, content_encoding="gzip"` — decompress the response body |
