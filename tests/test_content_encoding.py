"""Tests for content_encoding decompression in the resolve pipeline."""

from __future__ import annotations

import bz2
import gzip
import lzma
import zlib
from pathlib import Path

import pytest

from zmanifest import Builder, ContentEncoding, Manifest
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


class TestCompressOnIngest:
    """Tests for Builder.add(compress=...)."""

    PAYLOAD = b"the quick brown fox jumps over the lazy dog" * 100

    def test_compress_deflate(self, tmp_path: Path) -> None:
        """compress='deflate' compresses data and sets content_encoding."""
        builder = Builder()
        builder.add("/chunk.bin", data=self.PAYLOAD, compress="deflate")
        zmp_path = builder.write(tmp_path / "out.zmp")

        manifest = Manifest(str(zmp_path))
        entry = manifest.get_entry("/chunk.bin")
        assert entry.content_encoding == "deflate"
        assert entry.size == len(self.PAYLOAD)  # logical size
        assert entry.content_size < len(self.PAYLOAD)  # compressed

        import asyncio
        result = asyncio.run(resolve_entry(entry, manifest))
        assert result == self.PAYLOAD

    def test_compress_zstd(self, tmp_path: Path) -> None:
        builder = Builder()
        builder.add("/chunk.bin", data=self.PAYLOAD, compress=ContentEncoding.ZSTD)
        zmp_path = builder.write(tmp_path / "out.zmp")

        manifest = Manifest(str(zmp_path))
        entry = manifest.get_entry("/chunk.bin")
        assert entry.content_encoding == "zstd"

        import asyncio
        assert asyncio.run(resolve_entry(entry, manifest)) == self.PAYLOAD

    def test_compress_gzip(self, tmp_path: Path) -> None:
        builder = Builder()
        builder.add("/chunk.bin", data=self.PAYLOAD, compress=ContentEncoding.GZIP)
        zmp_path = builder.write(tmp_path / "out.zmp")

        manifest = Manifest(str(zmp_path))
        entry = manifest.get_entry("/chunk.bin")
        assert entry.content_encoding == "gzip"

        import asyncio
        assert asyncio.run(resolve_entry(entry, manifest)) == self.PAYLOAD

    def test_compress_lz4(self, tmp_path: Path) -> None:
        builder = Builder()
        builder.add("/chunk.bin", data=self.PAYLOAD, compress=ContentEncoding.LZ4)
        zmp_path = builder.write(tmp_path / "out.zmp")

        manifest = Manifest(str(zmp_path))
        entry = manifest.get_entry("/chunk.bin")
        assert entry.content_encoding == "lz4"

        import asyncio
        assert asyncio.run(resolve_entry(entry, manifest)) == self.PAYLOAD

    def test_compress_checksum_is_of_uncompressed(self, tmp_path: Path) -> None:
        """Checksum is computed from uncompressed data (before compression)."""
        from zmanifest.builder import git_blob_hash

        builder = Builder()
        builder.add("/chunk.bin", data=self.PAYLOAD, compress="deflate")
        zmp_path = builder.write(tmp_path / "out.zmp")

        manifest = Manifest(str(zmp_path))
        entry = manifest.get_entry("/chunk.bin")
        assert entry.checksum == git_blob_hash(self.PAYLOAD)

    def test_compress_enum(self, tmp_path: Path) -> None:
        """ContentEncoding enum works as compress value."""
        builder = Builder()
        builder.add("/a", data=self.PAYLOAD, compress=ContentEncoding.BR)
        zmp_path = builder.write(tmp_path / "out.zmp")

        manifest = Manifest(str(zmp_path))
        entry = manifest.get_entry("/a")
        assert entry.content_encoding == "br"

        import asyncio
        assert asyncio.run(resolve_entry(entry, manifest)) == self.PAYLOAD

    def test_compress_and_content_encoding_raises(self, tmp_path: Path) -> None:
        builder = Builder()
        with pytest.raises(ValueError, match="Cannot set both compress and content_encoding"):
            builder.add("/x", data=b"hello", compress="deflate", content_encoding="gzip")

    def test_compress_and_data_z_raises(self, tmp_path: Path) -> None:
        builder = Builder()
        with pytest.raises(ValueError, match="Cannot use compress with data_z"):
            builder.add("/x", data_z=b"hello", compress="deflate")

    def test_content_encoding_enum_for_precompressed(self, tmp_path: Path) -> None:
        """ContentEncoding enum works for pre-compressed data too."""
        compressed = gzip.compress(self.PAYLOAD)
        builder = Builder()
        builder.add(
            "/chunk.bin",
            data=compressed,
            content_encoding=ContentEncoding.GZIP,
            size=len(self.PAYLOAD),
        )
        zmp_path = builder.write(tmp_path / "out.zmp")

        manifest = Manifest(str(zmp_path))
        entry = manifest.get_entry("/chunk.bin")
        assert entry.content_encoding == "gzip"

        import asyncio
        assert asyncio.run(resolve_entry(entry, manifest)) == self.PAYLOAD
