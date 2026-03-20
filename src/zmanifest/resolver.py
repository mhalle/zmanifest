"""Built-in resolvers for common schemes."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

if TYPE_CHECKING:
    from vost import GitStore


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


class HttpResolver:
    """Resolves content via HTTP(S) URLs or local file paths.

    Handles relative URLs via base params, byte ranges via offset/length,
    and multipart/related responses (DICOMweb).

    Params: url, offset?, length?
    Base params: url (base URL for relative resolution)
    """

    async def resolve(self, params: dict, bases: list[dict] | None = None) -> bytes | None:
        from .resolve import _extract_multipart_frame

        url = params.get("url")
        if url is None:
            return None

        # Resolve relative URL against nearest base with a url key
        if bases and "://" not in url and not url.startswith("/"):
            for base in reversed(bases):  # innermost first
                if "url" in base:
                    base_url = base["url"]
                    if base_url.startswith(("http://", "https://")):
                        url = urljoin(base_url, url)
                    else:
                        url = os.path.normpath(os.path.join(base_url, url))
                    break

        # Local file path
        if not url.startswith(("http://", "https://")):
            path = url
            if path.startswith("file://"):
                path = path[7:]
            try:
                with open(path, "rb") as fh:
                    offset = params.get("offset")
                    length = params.get("length")
                    if offset is not None:
                        fh.seek(offset)
                    if length is not None:
                        return fh.read(length)
                    return fh.read()
            except FileNotFoundError:
                return None

        # HTTP fetch
        client = _get_http_client()
        offset = params.get("offset")
        length = params.get("length")
        if offset is not None and length is not None:
            headers = {"Range": f"bytes={offset}-{offset + length - 1}"}
            resp = await client.get(url, headers=headers)
        else:
            resp = await client.get(url)

        if resp.status_code not in (200, 206):
            return None

        ct = resp.headers.get("content-type", "")
        if "multipart/related" in ct:
            return _extract_multipart_frame(resp.content, ct)
        return resp.content


class GitResolver:
    """Resolves blobs from git repositories.

    Supports local bare repos (via vost/dulwich) and GitHub/GitLab
    repos (via raw file URL).

    Params: oid?, repo?, ref?, path?, offset?, length?
    Base params: repo, ref
    """

    def __init__(self, repo: GitStore | str | Path | None = None) -> None:
        self._default_repo = str(repo) if repo is not None else None
        self._stores: dict[str, Any] = {}

    def _get_store(self, repo_path: str) -> Any:
        if repo_path not in self._stores:
            from vost import GitStore as _GitStore
            self._stores[repo_path] = _GitStore.open(repo_path, create=False)
        return self._stores[repo_path]

    async def resolve(self, params: dict, bases: list[dict] | None = None) -> bytes | None:
        # Accumulate base chain: repo and ref from nearest ancestor
        repo = params.get("repo") or self._default_repo
        ref = params.get("ref")
        if bases:
            for base in reversed(bases):
                if repo is None or repo == self._default_repo:
                    repo = base.get("repo", repo)
                if ref is None:
                    ref = base.get("ref")
        if repo is None:
            return None

        # By object hash
        oid = params.get("oid")
        if oid is not None:
            if repo.startswith(("http://", "https://")):
                return await self._resolve_remote_oid(repo, oid)
            store = self._get_store(repo)
            if not store.has_hash(oid):
                return None
            data = store.read_by_hash(oid)
            return _apply_range(data, params)

        # By ref + path
        path = params.get("path")
        if path is not None:
            ref = ref or "HEAD"
            if repo.startswith(("http://", "https://")):
                return await self._resolve_remote_path(repo, ref, path, params)

        return None

    async def _resolve_remote_oid(self, repo_url: str, oid: str) -> bytes | None:
        return None  # no standard HTTP API for fetching by oid

    async def _resolve_remote_path(
        self, repo_url: str, ref: str, path: str, params: dict,
    ) -> bytes | None:
        """Resolve from GitHub/GitLab via raw file URL."""
        if "github.com" in repo_url:
            raw_url = repo_url.replace("github.com", "raw.githubusercontent.com")
            raw_url = raw_url.rstrip("/").removesuffix(".git")
            raw_url = f"{raw_url}/{ref}/{path}"
            client = _get_http_client()
            resp = await client.get(raw_url)
            if resp.status_code == 200:
                return _apply_range(resp.content, params)
        return None


class DicomWebResolver:
    """Resolves pixel data from DICOMweb WADO-RS endpoints.

    Params: url?, study, series, instance, frame?
    Base params: url (DICOMweb service base URL)
    """

    def __init__(self, headers: dict[str, str] | None = None) -> None:
        self._headers = headers or {}

    async def resolve(self, params: dict, bases: list[dict] | None = None) -> bytes | None:
        from .resolve import _extract_multipart_frame

        service_url = params.get("url")
        if service_url is None and bases:
            for base in reversed(bases):
                if "url" in base:
                    service_url = base["url"]
                    break
        if service_url is None:
            return None

        study = params.get("study")
        series = params.get("series")
        instance = params.get("instance")
        if not all([study, series, instance]):
            return None

        url = f"{service_url.rstrip('/')}/studies/{study}/series/{series}/instances/{instance}"
        frame = params.get("frame")
        if frame is not None:
            url += f"/frames/{frame}"

        client = _get_http_client()
        headers = dict(self._headers)
        headers.setdefault("Accept", "multipart/related; type=\"application/octet-stream\"")
        resp = await client.get(url, headers=headers)

        if resp.status_code not in (200, 206):
            return None

        ct = resp.headers.get("content-type", "")
        if "multipart/related" in ct:
            return _extract_multipart_frame(resp.content, ct)
        return resp.content


def _apply_range(data: bytes, params: dict) -> bytes:
    """Apply optional offset/length range to bytes."""
    offset = params.get("offset")
    length = params.get("length")
    if offset is not None:
        data = data[offset:]
    if length is not None:
        data = data[:length]
    return data


# Convenience alias
FileResolver = HttpResolver
