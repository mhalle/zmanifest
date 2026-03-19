from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from vost import GitStore


@runtime_checkable
class BlobResolver(Protocol):
    async def resolve(self, retrieval_key: str) -> bytes | None: ...


class GitResolver:
    """Resolves blobs from a vost GitStore (bare git repository).

    Requires the ``vost`` package (install with ``pip install zarr-manifest-parquet[git]``).
    Handles both loose and packed objects transparently via dulwich.
    """

    def __init__(self, store: GitStore | str | Path) -> None:
        from vost import GitStore as _GitStore

        if isinstance(store, _GitStore):
            self._store = store
        else:
            self._store = _GitStore.open(str(store), create=False)

    async def resolve(self, retrieval_key: str) -> bytes | None:
        if not self._store.has_hash(retrieval_key):
            return None
        return self._store.read_by_hash(retrieval_key)


class TemplateResolver:
    """Resolves blobs using a URL/path template with ``{hash}`` placeholder.

    For local paths, reads from the filesystem. For HTTP(S) URLs, fetches
    via httpx.

    Examples::

        TemplateResolver("/data/blobs/{hash}")
        TemplateResolver("/data/objects/{hash[:2]}/{hash[2:]}")
        TemplateResolver("https://cdn.example.com/blobs/{hash}")
    """

    def __init__(self, template: str) -> None:
        self._template = template
        self._is_http = template.startswith("http://") or template.startswith("https://")

    def _expand(self, retrieval_key: str) -> str:
        # Support {hash}, {hash[:2]}, {hash[2:]} style slicing
        import re

        def _replace(m: re.Match[str]) -> str:
            expr = m.group(1)
            if expr == "hash":
                return retrieval_key
            # Handle slice notation: hash[:2], hash[2:]
            slice_match = re.fullmatch(r"hash\[(-?\d*):(-?\d*)\]", expr)
            if slice_match:
                start = int(slice_match.group(1)) if slice_match.group(1) else None
                end = int(slice_match.group(2)) if slice_match.group(2) else None
                return retrieval_key[start:end]
            return m.group(0)

        return re.sub(r"\{([^}]+)\}", _replace, self._template)

    async def resolve(self, retrieval_key: str) -> bytes | None:
        location = self._expand(retrieval_key)

        if self._is_http:
            import httpx

            async with httpx.AsyncClient() as client:
                resp = await client.get(location)
                if resp.status_code == 200:
                    return resp.content
                return None
        else:
            try:
                with open(location, "rb") as f:
                    return f.read()
            except FileNotFoundError:
                return None


# Convenience aliases
FileResolver = TemplateResolver
HTTPResolver = TemplateResolver
