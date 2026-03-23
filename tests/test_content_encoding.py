"""Tests for content_encoding decompression in the resolve pipeline."""

from __future__ import annotations

import bz2
import gzip
import lzma
import zlib
from pathlib import Path

import pytest

from zmanifest import Builder, Manifest
from zmanifest.resolve import _decode_content, resolve_entry


class TestDecodeContent:
    """Unit tests for the _decode_content function."""

    PAYLOAD = b"hello world, this is test data for compression"

    def test_none_passthrough(self) -> None:
        assert _decode_content(self.PAYLOAD, None) == self.PAYLOAD

    def test_empty_passthrough(self) -> None:
        assert _decode_content(self.PAYLOAD, "") == self.PAYLOAD

    def test_deflate(self) -> None:
        compressed = zlib.compress(self.PAYLOAD)[2:-4]  # strip zlib header/trailer = raw deflate
        assert _decode_content(compressed, "deflate") == self.PAYLOAD

    def test_gzip(self) -> None:
        compressed = gzip.compress(self.PAYLOAD)
        assert _decode_content(compressed, "gzip") == self.PAYLOAD

    def test_zlib(self) -> None:
        compressed = zlib.compress(self.PAYLOAD)
        assert _decode_content(compressed, "zlib") == self.PAYLOAD

    def test_bz2(self) -> None:
        compressed = bz2.compress(self.PAYLOAD)
        assert _decode_content(compressed, "bz2") == self.PAYLOAD

    def test_lzma(self) -> None:
        compressed = lzma.compress(self.PAYLOAD)
        assert _decode_content(compressed, "lzma") == self.PAYLOAD

    def test_unsupported_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported content_encoding"):
            _decode_content(self.PAYLOAD, "rot13")


class TestResolveWithEncoding:
    """Integration tests: Builder -> Manifest -> resolve with content_encoding."""

    PAYLOAD = b"the quick brown fox jumps over the lazy dog" * 10

    def test_inline_data_deflate(self, tmp_path: Path) -> None:
        """Inline data with content_encoding='deflate' is decompressed on resolve."""
        compressed = zlib.compress(self.PAYLOAD)[2:-4]

        builder = Builder()
        builder.add(
            "/chunk.bin",
            data=compressed,
            content_encoding="deflate",
            size=len(self.PAYLOAD),
        )
        zmp_path = builder.write(tmp_path / "out.zmp")

        manifest = Manifest(str(zmp_path))
        entry = manifest.get_entry("/chunk.bin")
        assert entry.content_encoding == "deflate"

        import asyncio
        result = asyncio.run(resolve_entry(entry, manifest))
        assert result == self.PAYLOAD

    def test_inline_data_gzip(self, tmp_path: Path) -> None:
        compressed = gzip.compress(self.PAYLOAD)

        builder = Builder()
        builder.add(
            "/chunk.bin",
            data=compressed,
            content_encoding="gzip",
            size=len(self.PAYLOAD),
        )
        zmp_path = builder.write(tmp_path / "out.zmp")

        manifest = Manifest(str(zmp_path))
        entry = manifest.get_entry("/chunk.bin")

        import asyncio
        result = asyncio.run(resolve_entry(entry, manifest))
        assert result == self.PAYLOAD

    def test_inline_data_zstd(self, tmp_path: Path) -> None:
        pytest.importorskip("zstandard")
        import zstandard

        compressed = zstandard.compress(self.PAYLOAD)

        builder = Builder()
        builder.add(
            "/chunk.bin",
            data=compressed,
            content_encoding="zstd",
            size=len(self.PAYLOAD),
        )
        zmp_path = builder.write(tmp_path / "out.zmp")

        manifest = Manifest(str(zmp_path))
        entry = manifest.get_entry("/chunk.bin")

        import asyncio
        result = asyncio.run(resolve_entry(entry, manifest))
        assert result == self.PAYLOAD

    def test_no_encoding_passthrough(self, tmp_path: Path) -> None:
        """Without content_encoding, data passes through unchanged."""
        builder = Builder()
        builder.add("/chunk.bin", data=self.PAYLOAD)
        zmp_path = builder.write(tmp_path / "out.zmp")

        manifest = Manifest(str(zmp_path))
        entry = manifest.get_entry("/chunk.bin")

        import asyncio
        result = asyncio.run(resolve_entry(entry, manifest))
        assert result == self.PAYLOAD

    def test_text_not_decoded(self, tmp_path: Path) -> None:
        """Text entries ignore content_encoding (text is always UTF-8)."""
        builder = Builder()
        builder.add("/meta.json", text='{"key":"value"}')
        zmp_path = builder.write(tmp_path / "out.zmp")

        manifest = Manifest(str(zmp_path))
        entry = manifest.get_entry("/meta.json")

        import asyncio
        result = asyncio.run(resolve_entry(entry, manifest))
        assert b"key" in result
