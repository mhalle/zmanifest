"""Content resolution and URI handling for ZMP manifests."""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from .manifest import Manifest, ManifestEntry
from .resolver import BlobResolver


# Type alias for mount opener callbacks
MountOpener = Callable[[ManifestEntry], Any]


def is_relative_uri(uri: str) -> bool:
    """A URI is relative if it has no scheme and doesn't start with /."""
    return "://" not in uri and not uri.startswith("/")


def resolve_uri(uri: str, base_uri: str | None) -> str:
    """Resolve a possibly-relative URI against a base.

    Absolute URIs (have scheme or start with /) pass through unchanged.
    Relative URIs are joined against base_uri if available.
    """
    if base_uri is None or not is_relative_uri(uri):
        return uri
    if base_uri.startswith(("http://", "https://")):
        return urljoin(base_uri, uri)
    return os.path.normpath(os.path.join(base_uri, uri))


def base_uri_from_source(source: str) -> str:
    """Derive a base URI (parent directory) from a manifest URL or path."""
    if source.startswith(("http://", "https://")):
        return source.rsplit("/", 1)[0] + "/"
    return str(Path(source).resolve().parent)


_http_client = None


def _get_http_client() -> Any:
    """Return a shared httpx.AsyncClient with connection pooling."""
    global _http_client
    if _http_client is None:
        import httpx
        _http_client = httpx.AsyncClient(
            timeout=60,
            http2=True,
        )
    return _http_client


def _extract_multipart_frame(body: bytes, content_type: str) -> bytes | None:
    """Extract the octet-stream part from a multipart/related response.

    Uses find-based scanning to avoid copying the body.
    """
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
            while end > data_start and body[end - 1] in (13, 10):  # \r \n
                end -= 1
            return body[data_start:end]
        pos = body.find(marker, header_start)
    return None


async def fetch_uri(uri: str, offset: int | None, length: int | None) -> bytes | None:
    """Fetch bytes from a local path or HTTP(S) URL with optional byte range."""
    if uri.startswith(("http://", "https://")):
        client = _get_http_client()
        if offset is not None and length is not None:
            headers = {"Range": f"bytes={offset}-{offset + length - 1}"}
            resp = await client.get(uri, headers=headers)
        else:
            resp = await client.get(uri)
        if resp.status_code in (200, 206):
            ct = resp.headers.get("content-type", "")
            if "multipart/related" in ct:
                return _extract_multipart_frame(resp.content, ct)
            return resp.content
        return None
    else:
        path = uri
        if path.startswith("file://"):
            path = path[7:]
        try:
            with open(path, "rb") as fh:
                if offset is not None:
                    fh.seek(offset)
                if length is not None:
                    return fh.read(length)
                return fh.read()
        except FileNotFoundError:
            return None


async def resolve_entry(
    entry: ManifestEntry,
    manifest: Manifest,
    resolver: BlobResolver | None,
    base_uri: str | None = None,
    _visited: set[str] | None = None,
) -> bytes | None:
    """Resolve content following the spec resolution order.

    Uses the entry's addressing flags to skip resolution steps that
    can't succeed, avoiding unnecessary work.

    Resolution order:
    1. Inline text (T)
    2. Inline data (D)
    3. Content-addressed lookup via retrieval_key (K)
    4. Link — follow target path in the same manifest (L)
    5. External URI with optional byte range (U)
    """
    from ._types import Addressing

    flags = entry.addressing

    # 1. Inline text
    if Addressing.TEXT in flags:
        return entry.text.encode("utf-8")

    # 2. Inline data (binary)
    if Addressing.DATA in flags:
        data = manifest.get_data(entry.path)
        if data is not None:
            return data

    # 3. Content-addressed lookup via retrieval_key
    if Addressing.KEY in flags and resolver is not None:
        blob = await resolver.resolve(entry.retrieval_key)
        if blob is not None:
            return blob

    # 4. Link — follow target path in the same manifest
    if Addressing.LINK in flags and entry.uri is not None:
        if _visited is None:
            _visited = set()
        if entry.path in _visited:
            raise ValueError(
                f"Circular link detected: {entry.path!r} -> {entry.uri!r}"
            )
        _visited.add(entry.path)
        target_entry = manifest.get_entry(entry.uri)
        if target_entry is not None:
            target_base = target_entry.base_uri or base_uri
            return await resolve_entry(
                target_entry, manifest, resolver, target_base, _visited,
            )

    # 5. External URI (with optional byte range)
    if Addressing.URI in flags:
        uri = resolve_uri(entry.uri, base_uri)
        blob = await fetch_uri(uri, entry.offset, entry.length)
        if blob is not None:
            return blob

    return None
