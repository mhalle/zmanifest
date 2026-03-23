# URL Resolution and Base Resolve Chain

## Overview

When a manifest entry references external data (via `resolve`), the
`HttpResolver` composes a full URL from the entry's params and a chain
of base URLs. This document explains how that composition works.

## The base_resolve chain

The chain is ordered outermost to innermost:

1. **Location base** — the manifest file's parent directory (added by
   `ZMPStore.from_file()` only when the manifest has no file-level
   `base_resolve`)
2. **File-level base** — from the manifest's parquet metadata
   (`base_resolve` key)
3. **Entry-level base** — from the entry's `base_resolve` column

Each layer can override or extend the previous. Absolute URLs or
paths (starting with `/` or containing `://`) override everything
before them.

## Two access patterns

### Pattern 1: Relative URL resolution

The entry has a relative URL, the base is a directory:

```python
# File-level base_resolve
base_resolve = {"http": {"url": "https://cdn.example.com/data/"}}

# Entry resolve
resolve = {"http": {"url": "chunks/abc123"}}

# Result: https://cdn.example.com/data/chunks/abc123
```

The base URL must end with `/` to be treated as a directory. The
entry URL is joined relative to it.

### Pattern 2: Byte-range access into a file

The entry has offset/length but no URL. The base IS the file:

```python
# File-level base_resolve (set by Builder)
base_resolve = {"http": {"url": "/data/archive.tif"}}

# Entry resolve (no url, just offset/length)
resolve = {"http": {"offset": 1234, "length": 5678}}

# Result: read bytes 1234-6911 from /data/archive.tif
```

The resolver uses the base URL directly as the file path. No path
joining occurs.

This pattern is used by `zmp import-zip --virtual` and TIFF virtual
manifests where all data references point into a single file at
different byte offsets.

## Composition rules

The resolver processes the base chain outermost to innermost:

```
effective_base = None
for each base in chain:
    base_url = base["url"]
    if base_url is absolute (starts with / or has ://):
        effective_base = base_url    # override
    else:
        effective_base = join(effective_base, base_url)  # extend
```

Then:
- If entry has a `url`: join it against `effective_base`
- If entry has no `url` (just offset/length): use `effective_base` as-is

## When is the location base added?

`ZMPStore.from_file()` adds the manifest's parent directory as a base
**only when the manifest has no file-level `base_resolve`**. This
covers the common case where relative URLs in entries should resolve
against the manifest's location:

```
# Manifest at /data/dataset/manifest.zmp
# Entry with resolve: {"http": {"url": "chunks/abc123"}}
# Location base: /data/dataset/
# Resolves to: /data/dataset/chunks/abc123
```

When the manifest has its own `base_resolve` (e.g. pointing to a TIFF
or a remote URL), the location base is not added — the manifest knows
where its data lives.

## Examples

### Self-contained manifest with sibling blobs

```python
# /data/dataset/manifest.zmp
# /data/dataset/blobs/abc123
# /data/dataset/blobs/def456

# No file-level base_resolve → location base = /data/dataset/
builder = Builder()
builder.add("/chunk/0", url="blobs/abc123")
builder.add("/chunk/1", url="blobs/def456")
```

### Virtual TIFF manifest

```python
# Manifest knows the TIFF path
builder = Builder(
    base_resolve={"http": {"url": "/images/scan.tif"}}
)
# Each entry: byte range into the TIFF
builder.add("/volume/c/0/0/0",
    resolve={"http": {"offset": 288, "length": 134987}},
    content_encoding="zlib",
    size=261456,
)
```

### Virtual zip manifest

```python
# Remote zip file
builder = Builder(
    base_resolve={"http": {"url": "https://example.com/data.zip"}}
)
builder.add("/arr/c/0",
    resolve={"http": {"offset": 1234, "length": 5678}},
    content_encoding="deflate",
    size=65536,
)
```

### Mount with base override

```python
builder = Builder()
builder.add("/zarr.json", text='...')
builder.mount("remote/scan",
    resolve={"http": {"url": "https://cdn.example.com/scan.zmp"}},
    base_resolve={"http": {"url": "https://cdn.example.com/blobs/"}},
)
# Entries inside the mount resolve chunks against the CDN blob store,
# not the manifest's location.
```

## Content encoding

When `content_encoding` is set on an entry, the resolver decompresses
the fetched bytes automatically. This is independent of URL resolution —
it happens after the bytes are fetched regardless of how the URL was
composed.

Supported encodings: `deflate`, `gzip`, `zlib`, `bz2`, `lzma`, `zstd`,
`lz4`, `br`.

## Edge chunk padding

When reading virtual references through zarr, edge chunks (at array
boundaries) may decompress to fewer bytes than a full chunk. The
`ZMPStore` pads these to the full chunk size with the fill value (zero)
before returning to zarr. This only applies when `content_encoding` is
set — zarr-compressed chunks are already correctly sized.
