"""Tests for HttpResolver with base_resolve and no entry url."""

from __future__ import annotations

import asyncio
from pathlib import Path

import numpy as np
import pytest

from zmanifest.resolver import HttpResolver


class TestHttpResolverBaseOnly:
    """When entry has offset/length but no url, the base provides the file."""

    def test_local_file_base_with_offset(self, tmp_path: Path) -> None:
        """Base points to a local file, entry has offset+length only."""
        # Create a file with known content
        data = b"HEADER" + b"\x00" * 100 + b"PAYLOAD_HERE"
        f = tmp_path / "archive.bin"
        f.write_bytes(data)

        resolver = HttpResolver()

        # Entry has no url, just offset+length
        params = {"offset": 106, "length": 12}
        bases = [{"url": str(f)}]

        result = asyncio.run(resolver.resolve(params, bases))
        assert result == b"PAYLOAD_HERE"

    def test_local_file_base_no_range(self, tmp_path: Path) -> None:
        """Base points to a local file, entry has no url and no range."""
        data = b"full file content"
        f = tmp_path / "data.bin"
        f.write_bytes(data)

        resolver = HttpResolver()
        params = {}
        bases = [{"url": str(f)}]

        result = asyncio.run(resolver.resolve(params, bases))
        assert result == data

    def test_no_url_no_base_returns_none(self) -> None:
        """No url and no base → None."""
        resolver = HttpResolver()
        result = asyncio.run(resolver.resolve({}))
        assert result is None

    def test_no_url_no_base_with_offset_returns_none(self) -> None:
        """Offset/length without url or base → None."""
        resolver = HttpResolver()
        result = asyncio.run(resolver.resolve({"offset": 0, "length": 10}))
        assert result is None
