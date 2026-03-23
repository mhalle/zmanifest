"""Content resolution for ZMP manifests using pluggable scheme resolvers."""

from __future__ import annotations

import bz2
import gzip
import json
import lzma
import zlib
from typing import Any, Protocol, runtime_checkable

from .manifest import Manifest, ManifestEntry


# ---------------------------------------------------------------------------
# Content decoding (transport-level decompression)
# ---------------------------------------------------------------------------

# Maps content_encoding values to decompressor functions.
# These handle data that arrived already compressed from an external source
# (zip files, HTTP responses, pre-compressed blobs) — not to be confused
# with parquet column compression (data_z) or zarr codec pipelines.
_DECOMPRESSORS: dict[str, Any] = {
    # Raw deflate (zip default, HTTP Content-Encoding: deflate)
    "deflate": lambda data: zlib.decompress(data, -15),
    # Gzip (HTTP Content-Encoding: gzip, .gz files)
    "gzip": gzip.decompress,
    # Zlib (deflate + zlib header)
    "zlib": zlib.decompress,
    # Bzip2 (zip method 12, .bz2 files)
    "bz2": bz2.decompress,
    # LZMA (zip method 14, .xz/.lzma files)
    "lzma": lzma.decompress,
}

import brotli
import lz4.frame
import zstandard

_DECOMPRESSORS["zstd"] = zstandard.decompress
_DECOMPRESSORS["lz4"] = lz4.frame.decompress
_DECOMPRESSORS["br"] = brotli.decompress

# Compressors — used by Builder.add(compress=...) to compress on ingest
_COMPRESSORS: dict[str, Any] = {
    "deflate": lambda data: zlib.compress(data)[2:-4],  # strip zlib header/trailer
    "gzip": gzip.compress,
    "zlib": zlib.compress,
    "bz2": bz2.compress,
    "lzma": lzma.compress,
    "zstd": zstandard.compress,
    "lz4": lz4.frame.compress,
    "br": brotli.compress,
}


def _encode_content(data: bytes, encoding: str) -> bytes:
    """Compress data with the given encoding.

    Raises ValueError for unsupported encodings.
    """
    compressor = _COMPRESSORS.get(encoding)
    if compressor is None:
        raise ValueError(
            f"Unsupported content_encoding: {encoding!r}. "
            f"Available: {', '.join(sorted(_COMPRESSORS))}"
        )
    return compressor(data)


def _decode_content(data: bytes, encoding: str | None) -> bytes:
    """Decompress data based on content_encoding.

    If encoding is None or empty, returns data unchanged.
    Raises ValueError for unsupported encodings.
    """
    if not encoding:
        return data
    decompressor = _DECOMPRESSORS.get(encoding)
    if decompressor is None:
        raise ValueError(
            f"Unsupported content_encoding: {encoding!r}. "
            f"Available: {', '.join(sorted(_DECOMPRESSORS))}"
        )
    return decompressor(data)


@runtime_checkable
class Resolver(Protocol):
    """Protocol for scheme-specific content resolvers.

    Args:
        params: Scheme-specific params from the entry's resolve dict.
        bases: Chain of base_resolve dicts for this scheme, ordered
            from outermost (file-level) to innermost (nearest parent).
            The resolver merges them however makes sense for the scheme.
    """

    async def resolve(self, params: dict, bases: list[dict] | None = None) -> bytes | None: ...


def _extract_multipart_frame(body: bytes, content_type: str) -> bytes | None:
    """Extract the octet-stream part from a multipart/related response."""
    try:
        boundary = content_type.split("boundary=")[1].split(";")[0].strip()
    except IndexError:
        return body
    marker = f"--{boundary}".encode()
    pos = body.find(marker)
    while pos >= 0:
        header_start = pos + len(marker)
        header_end = body.find(b"\r\n\r\n", header_start)
        if header_end > 0 and b"octet-stream" in body[header_start:header_end]:
            data_start = header_end + 4
            next_boundary = body.find(marker, data_start)
            end = next_boundary if next_boundary >= 0 else len(body)
            while end > data_start and body[end - 1] in (13, 10):
                end -= 1
            return body[data_start:end]
        pos = body.find(marker, header_start)
    return None


def get_file_base_resolve(manifest: Manifest) -> dict | None:
    """Get the file-level base_resolve from parquet metadata."""
    extra = manifest.metadata.get("extra", {})
    raw = extra.get("base_resolve") if extra else None
    if raw is None:
        return None
    return json.loads(raw) if isinstance(raw, str) else raw


def build_base_chain(
    *layers: dict | str | None,
) -> list[dict]:
    """Build the base_resolve chain from outermost to innermost.

    Each layer is a base_resolve dict (or JSON string, or None).
    None layers are skipped.
    """
    chain: list[dict] = []
    for layer in layers:
        if layer is None:
            continue
        if isinstance(layer, str):
            layer = json.loads(layer)
        chain.append(layer)
    return chain


def _collect_scheme_bases(scheme: str, base_chain: list[dict] | None) -> list[dict]:
    """Extract per-scheme base dicts from the chain."""
    if not base_chain:
        return []
    bases = []
    for layer in base_chain:
        if scheme in layer:
            bases.append(layer[scheme])
    return bases


async def resolve_entry(
    entry: ManifestEntry,
    manifest: Manifest,
    resolvers: dict[str, Resolver] | None = None,
    base_resolve: list[dict] | None = None,
    _visited: set[str] | None = None,
) -> bytes | None:
    """Resolve content for a manifest entry.

    Resolution order:
    1. Inline text (T)
    2. Inline data (D)
    3. Link — follow target path in the same manifest (L)
    4. Resolve — iterate schemes in the resolve dict (R)

    Args:
        entry: The manifest entry to resolve.
        manifest: The manifest containing the entry.
        resolvers: Dict of scheme name -> Resolver instance.
        base_resolve: Chain of base_resolve dicts, outermost to innermost.
        _visited: Set of visited paths for cycle detection (internal).
    """
    from ._types import Addressing

    flags = entry.addressing
    encoding = entry.content_encoding

    # 1. Inline text
    if Addressing.TEXT in flags and entry.text is not None:
        return entry.text.encode("utf-8")

    # 2. Inline data (binary — uncompressed or compressed column)
    if Addressing.DATA in flags or Addressing.DATA_Z in flags:
        data = manifest.get_data(entry.path)
        if data is not None:
            return _decode_content(data, encoding)

    # 3. Link — follow _path target
    if Addressing.LINK in flags and entry.resolve is not None:
        resolve_dict = json.loads(entry.resolve) if isinstance(entry.resolve, str) else entry.resolve
        path_params = resolve_dict.get("_path")
        if path_params and "target" in path_params:
            if _visited is None:
                _visited = set()
            if entry.path in _visited:
                raise ValueError(
                    f"Circular link detected: {entry.path!r} -> {path_params['target']!r}"
                )
            _visited.add(entry.path)
            target_entry = manifest.get_entry(path_params["target"])
            if target_entry is not None:
                target_chain = list(base_resolve or [])
                if target_entry.base_resolve:
                    target_br = json.loads(target_entry.base_resolve) if isinstance(target_entry.base_resolve, str) else target_entry.base_resolve
                    target_chain.append(target_br)
                return await resolve_entry(
                    target_entry, manifest, resolvers, target_chain or None, _visited,
                )

    # 4. Resolve — try each scheme
    if Addressing.RESOLVE in flags and entry.resolve is not None and resolvers:
        resolve_dict = json.loads(entry.resolve) if isinstance(entry.resolve, str) else entry.resolve
        for scheme, params in resolve_dict.items():
            if scheme.startswith("_"):
                continue
            resolver = resolvers.get(scheme)
            if resolver is None:
                continue
            scheme_bases = _collect_scheme_bases(scheme, base_resolve)
            result = await resolver.resolve(params, scheme_bases or None)
            if result is not None:
                return _decode_content(result, encoding)

    return None
