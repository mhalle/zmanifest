"""Content resolution for ZMP manifests using pluggable scheme resolvers."""

from __future__ import annotations

import json
from typing import Any, Protocol, runtime_checkable

from .manifest import Manifest, ManifestEntry


@runtime_checkable
class Resolver(Protocol):
    """Protocol for scheme-specific content resolvers."""

    async def resolve(self, params: dict, base: dict | None = None) -> bytes | None: ...


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


def merge_base_resolve(
    base: dict | None,
    override: dict | None,
) -> dict | None:
    """Shallow merge of base_resolve dicts. Override replaces base per scheme."""
    if base is None:
        return override
    if override is None:
        return base
    merged = dict(base)
    merged.update(override)
    return merged


def get_base_resolve(
    manifest: Manifest,
    entry: ManifestEntry | None = None,
) -> dict | None:
    """Get the effective base_resolve for an entry.

    Merges file-level base_resolve with the entry's own base_resolve.
    """
    # File-level base_resolve from parquet metadata
    extra = manifest.metadata.get("extra", {})
    file_base_str = extra.get("base_resolve") if extra else None
    file_base = json.loads(file_base_str) if isinstance(file_base_str, str) else file_base_str

    if entry is None or entry.base_resolve is None:
        return file_base

    entry_base = json.loads(entry.base_resolve) if isinstance(entry.base_resolve, str) else entry.base_resolve
    return merge_base_resolve(file_base, entry_base)


async def resolve_entry(
    entry: ManifestEntry,
    manifest: Manifest,
    resolvers: dict[str, Resolver] | None = None,
    base_resolve: dict | None = None,
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
        base_resolve: Base resolution params (merged file + parent).
        _visited: Set of visited paths for cycle detection (internal).
    """
    from ._types import Addressing

    flags = entry.addressing

    # 1. Inline text
    if Addressing.TEXT in flags and entry.text is not None:
        return entry.text.encode("utf-8")

    # 2. Inline data (binary)
    if Addressing.DATA in flags:
        data = manifest.get_data(entry.path)
        if data is not None:
            return data

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
                target_base = merge_base_resolve(
                    base_resolve,
                    json.loads(target_entry.base_resolve) if target_entry.base_resolve else None,
                )
                return await resolve_entry(
                    target_entry, manifest, resolvers, target_base, _visited,
                )

    # 4. Resolve — try each scheme
    if Addressing.RESOLVE in flags and entry.resolve is not None and resolvers:
        resolve_dict = json.loads(entry.resolve) if isinstance(entry.resolve, str) else entry.resolve
        for scheme, params in resolve_dict.items():
            if scheme.startswith("_"):
                continue  # skip internal schemes
            resolver = resolvers.get(scheme)
            if resolver is None:
                continue
            scheme_base = base_resolve.get(scheme) if base_resolve else None
            result = await resolver.resolve(params, scheme_base)
            if result is not None:
                return result

    return None
