"""Tests for Manifest.get_data_location() — direct byte-range access."""

from __future__ import annotations

import struct
from pathlib import Path

import numpy as np
import pytest

from zmanifest import Builder, Manifest


def _verify_location(zmp_path: str, path: str, expected_data: bytes) -> None:
    """Helper: verify get_data_location returns correct offset/length."""
    m = Manifest(zmp_path)
    loc = m.get_data_location(path)
    assert loc is not None, f"get_data_location returned None for {path}"
    offset, length = loc
    assert length == len(expected_data), f"length mismatch: {length} != {len(expected_data)}"
    with open(zmp_path, "rb") as f:
        f.seek(offset)
        actual = f.read(length)
    assert actual == expected_data, f"data mismatch at offset {offset}"


class TestBasicLocation:
    def test_single_small_blob(self, tmp_path: Path) -> None:
        data = b"\xAA" * 100
        builder = Builder()
        builder.add("/chunk", data=data)
        zmp = str(builder.write(tmp_path / "out.zmp"))
        _verify_location(zmp, "/chunk", data)

    def test_single_large_blob(self, tmp_path: Path) -> None:
        data = np.random.bytes(1_000_000)
        builder = Builder()
        builder.add("/chunk", data=data)
        zmp = str(builder.write(tmp_path / "out.zmp"))
        _verify_location(zmp, "/chunk", data)

    def test_multiple_blobs(self, tmp_path: Path) -> None:
        blobs = {}
        builder = Builder()
        for i in range(10):
            data = np.random.bytes(500 + i * 200)
            builder.add(f"/c/{i}", data=data)
            blobs[f"/c/{i}"] = data
        zmp = str(builder.write(tmp_path / "out.zmp"))

        for path, expected in blobs.items():
            _verify_location(zmp, path, expected)

    def test_varying_sizes(self, tmp_path: Path) -> None:
        """Blobs from 1 byte to 100KB."""
        sizes = [1, 10, 100, 1000, 10000, 100000]
        blobs = {}
        builder = Builder()
        for i, sz in enumerate(sizes):
            data = np.random.bytes(sz)
            builder.add(f"/c/{i}", data=data)
            blobs[f"/c/{i}"] = data
        zmp = str(builder.write(tmp_path / "out.zmp"))

        for path, expected in blobs.items():
            _verify_location(zmp, path, expected)

    def test_empty_blob(self, tmp_path: Path) -> None:
        """Zero-length blob."""
        builder = Builder()
        builder.add("/empty", data=b"")
        zmp = str(builder.write(tmp_path / "out.zmp"))

        m = Manifest(zmp)
        loc = m.get_data_location("/empty")
        if loc is not None:
            offset, length = loc
            assert length == 0


class TestMixedContent:
    def test_text_and_data(self, tmp_path: Path) -> None:
        """Text entries return None, data entries return location."""
        data = b"\x00" * 500
        builder = Builder()
        builder.add("/zarr.json", text='{"zarr_format":3}')
        builder.add("/chunk", data=data)
        zmp = str(builder.write(tmp_path / "out.zmp"))

        m = Manifest(zmp)
        assert m.get_data_location("/zarr.json") is None
        _verify_location(zmp, "/chunk", data)

    def test_resolve_entries(self, tmp_path: Path) -> None:
        """Resolve-only entries return None."""
        builder = Builder()
        builder.add("/ref", url="https://example.com/data", size=100)
        zmp = str(builder.write(tmp_path / "out.zmp"))

        m = Manifest(zmp)
        assert m.get_data_location("/ref") is None

    def test_data_with_text_interleaved(self, tmp_path: Path) -> None:
        """Data entries work even when interleaved with text entries."""
        blobs = {}
        builder = Builder()
        builder.add("/zarr.json", text='{}')
        for i in range(5):
            builder.add(f"/group/zarr.json", text='{}') if i == 0 else None
            data = np.random.bytes(1000)
            builder.add(f"/group/c/{i}", data=data)
            blobs[f"/group/c/{i}"] = data
        zmp = str(builder.write(tmp_path / "out.zmp"))

        for path, expected in blobs.items():
            _verify_location(zmp, path, expected)


class TestMultipleRowGroups:
    def test_many_blobs_across_row_groups(self, tmp_path: Path) -> None:
        """Enough data to span multiple adaptive row groups."""
        blobs = {}
        builder = Builder()
        # 50 x 200KB = 10MB → should trigger at least one row group boundary
        for i in range(50):
            data = np.random.bytes(200_000)
            builder.add(f"/c/{i}", data=data)
            blobs[f"/c/{i}"] = data
        zmp = str(builder.write(tmp_path / "out.zmp"))

        m = Manifest(zmp)
        pf = m._pf
        print(f"Row groups: {pf.metadata.num_row_groups}")

        for path, expected in blobs.items():
            _verify_location(zmp, path, expected)

    def test_first_and_last_blob(self, tmp_path: Path) -> None:
        """First and last blobs in a multi-RG file."""
        blobs = {}
        builder = Builder()
        for i in range(20):
            data = np.random.bytes(100_000)
            builder.add(f"/c/{i}", data=data)
            blobs[f"/c/{i}"] = data
        zmp = str(builder.write(tmp_path / "out.zmp"))

        _verify_location(zmp, "/c/0", blobs["/c/0"])
        _verify_location(zmp, "/c/19", blobs["/c/19"])


class TestEdgeCases:
    def test_nonexistent_path(self, tmp_path: Path) -> None:
        builder = Builder()
        builder.add("/chunk", data=b"\x00")
        zmp = str(builder.write(tmp_path / "out.zmp"))

        m = Manifest(zmp)
        assert m.get_data_location("/nope") is None

    def test_archive_row(self, tmp_path: Path) -> None:
        builder = Builder()
        builder.set_archive_metadata({"test": True})
        builder.add("/chunk", data=b"\x00")
        zmp = str(builder.write(tmp_path / "out.zmp"))

        m = Manifest(zmp)
        assert m.get_data_location("") is None

    def test_folder_entry(self, tmp_path: Path) -> None:
        builder = Builder()
        builder.add("/group", is_folder=True)
        builder.add("/chunk", data=b"\x00")
        zmp = str(builder.write(tmp_path / "out.zmp"))

        m = Manifest(zmp)
        assert m.get_data_location("/group") is None

    def test_accepts_bare_and_absolute_paths(self, tmp_path: Path) -> None:
        data = b"\xFF" * 100
        builder = Builder()
        builder.add("/arr/c/0", data=data)
        zmp = str(builder.write(tmp_path / "out.zmp"))

        m = Manifest(zmp)
        loc1 = m.get_data_location("/arr/c/0")
        loc2 = m.get_data_location("arr/c/0")
        assert loc1 is not None
        assert loc1 == loc2

    def test_bytes_manifest_returns_none(self, tmp_path: Path) -> None:
        """Manifests loaded from bytes can't do file seeks."""
        builder = Builder()
        builder.add("/chunk", data=b"\x00" * 100)
        zmp_path = builder.write(tmp_path / "out.zmp")

        zmp_bytes = Path(zmp_path).read_bytes()
        m = Manifest(zmp_bytes)
        assert m.get_data_location("/chunk") is None


class TestConsistencyWithGetData:
    def test_location_matches_get_data(self, tmp_path: Path) -> None:
        """Verify get_data_location and get_data return the same bytes."""
        builder = Builder()
        for i in range(20):
            builder.add(f"/c/{i}", data=np.random.bytes(1000 + i * 100))
        zmp = str(builder.write(tmp_path / "out.zmp"))

        m = Manifest(zmp)
        for i in range(20):
            path = f"/c/{i}"
            blob = m.get_data(path)
            loc = m.get_data_location(path)
            assert loc is not None
            offset, length = loc
            with open(zmp, "rb") as f:
                f.seek(offset)
                direct = f.read(length)
            assert direct == blob, f"mismatch at {path}"
