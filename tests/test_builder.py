"""Tests for Builder."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pytest
import pyarrow.parquet as pq
import rfc8785

from zmanifest import Addressing, Builder, Manifest
from zmanifest.builder import canonical_json, git_blob_hash


class TestBuilder:
    def test_inline_roundtrip(self, tmp_path: Path) -> None:
        """Inline text + data entries are retrievable."""
        builder = Builder()
        builder.add("zarr.json", text='{"zarr_format":3,"node_type":"group"}')
        builder.add("arr/zarr.json", text='{"zarr_format":3,"node_type":"array"}')

        chunk = np.array([1.0, 2.0], dtype="<f8").tobytes()
        builder.add("arr/c/0", data=chunk)

        zmp_path = builder.write(tmp_path / "out.zmp")
        manifest = Manifest(str(zmp_path))

        entry = manifest.get_entry("zarr.json")
        assert entry is not None
        assert json.loads(entry.text)["node_type"] == "group"

        data = manifest.get_data("arr/c/0")
        assert data == chunk

    def test_virtual_refs(self, tmp_path: Path) -> None:
        """Virtual entries have resolve dict."""
        builder = Builder()
        builder.add("zarr.json", text='{}')
        builder.add(
            "arr/c/0",
            resolve={"http": {"url": "s3://bucket/file.nc", "offset": 1024, "length": 4096}},
            size=4096,
        )
        zmp_path = builder.write(tmp_path / "out.zmp")
        manifest = Manifest(str(zmp_path))

        entry = manifest.get_entry("arr/c/0")
        assert entry is not None
        resolve = json.loads(entry.resolve)
        assert resolve["http"]["url"] == "s3://bucket/file.nc"
        assert resolve["http"]["offset"] == 1024
        assert entry.size == 4096

    def test_ref_entries(self, tmp_path: Path) -> None:
        """Reference entries have checksum but no inline data."""
        builder = Builder()
        builder.add("zarr.json", text='{}')
        builder.add(
            "arr/c/0",
            resolve={"git": {"oid": "a" * 40}},
            checksum="a" * 40,
            size=32768,
        )
        zmp_path = builder.write(tmp_path / "out.zmp")
        manifest = Manifest(str(zmp_path))

        entry = manifest.get_entry("arr/c/0")
        assert entry.checksum == "a" * 40
        assert entry.size == 32768
        assert entry.text is None
        assert manifest.get_data("arr/c/0") is None

    def test_json_text_canonicalized(self, tmp_path: Path) -> None:
        """JSON text is canonicalized (RFC 8785) before hashing."""
        text = '{"b": 1, "a": 2}'
        builder = Builder()
        builder.add("zarr.json", text=text)
        zmp_path = builder.write(tmp_path / "out.zmp")

        manifest = Manifest(str(zmp_path))
        entry = manifest.get_entry("zarr.json")

        canonical = rfc8785.dumps(json.loads(text))
        header = f"blob {len(canonical)}\0".encode()
        expected = hashlib.sha1(header + canonical).hexdigest()

        assert entry.checksum == expected
        assert entry.text == canonical.decode("utf-8")

    def test_data_hash_is_raw(self, tmp_path: Path) -> None:
        """Data hashes are git-sha1 of raw bytes."""
        data = b"\x00\x01\x02\x03"
        builder = Builder()
        builder.add("c/0", data=data)
        zmp_path = builder.write(tmp_path / "out.zmp")

        manifest = Manifest(str(zmp_path))
        entry = manifest.get_entry("c/0")

        header = f"blob {len(data)}\0".encode()
        expected = hashlib.sha1(header + data).hexdigest()
        assert entry.checksum == expected

    def test_no_array_path_column(self, tmp_path: Path) -> None:
        """Builder does not write zarr-specific columns."""
        builder = Builder()
        builder.add("myarray/c/0/1", data=b"\x00")
        zmp_path = builder.write(tmp_path / "out.zmp")

        pf = pq.ParquetFile(str(zmp_path))
        assert "array_path" not in pf.schema_arrow.names
        assert "chunk_key" not in pf.schema_arrow.names

    def test_sorted_output(self, tmp_path: Path) -> None:
        """Archive row first, then non-data (text), then data rows."""
        builder = Builder()
        builder.add("zarr.json", text='{}')
        builder.add("b/c/0", data=b"\x01")
        builder.add("a/c/0", data=b"\x00")

        zmp_path = builder.write(tmp_path / "out.zmp")
        manifest = Manifest(str(zmp_path))
        paths = list(manifest.list_paths())
        # archive first, then non-data sorted, then data sorted
        assert paths == ["", "zarr.json", "a/c/0", "b/c/0"]

    def test_file_level_metadata(self, tmp_path: Path) -> None:
        builder = Builder(
            zarr_format="3",
            metadata={"description": "test", "doi": "10.1234/test"},
        )
        builder.add("zarr.json", text='{}')
        zmp_path = builder.write(tmp_path / "out.zmp")

        manifest = Manifest(str(zmp_path))
        assert manifest.metadata["zmp_version"] == "0.2.0"
        assert manifest.metadata["zarr_format"] == "3"
        assert manifest.metadata.get("extra", {}).get("description") == "test"
        assert manifest.metadata.get("extra", {}).get("doi") == "10.1234/test"

    def test_row_group_sizing_inline(self, tmp_path: Path) -> None:
        """Inline data row groups are adaptively sized."""
        builder = Builder()
        builder.add("zarr.json", text='{}')
        for i in range(10):
            builder.add(f"c/{i}", data=b"\x00" * 100)
        zmp_path = builder.write(tmp_path / "out.zmp")

        pf = pq.ParquetFile(str(zmp_path))
        # 10 x 100 bytes = 1KB total data, well under 10MB target
        # so all 10 data rows should be in one row group,
        # plus 1 for non-data (zarr.json), plus 1 for archive row
        assert pf.metadata.num_row_groups == 3

    def test_row_group_sizing_refs(self, tmp_path: Path) -> None:
        """Reference-only uses larger row groups."""
        builder = Builder()
        for i in range(1000):
            builder.add(f"c/{i}", resolve={"git": {"oid": "a" * 40}}, checksum="a" * 40, size=100)
        zmp_path = builder.write(tmp_path / "out.zmp")

        pf = pq.ParquetFile(str(zmp_path))
        assert pf.metadata.num_row_groups <= 20

    def test_mixed_inline_and_virtual(self, tmp_path: Path) -> None:
        """Mix of inline data and virtual resolve entries."""
        builder = Builder()
        builder.add("zarr.json", text='{"zarr_format":3,"node_type":"group"}')
        builder.add("native/c/0", data=b"\x00" * 16)
        builder.add(
            "virtual/c/0",
            resolve={"http": {"url": "https://example.com/data.bin", "offset": 0, "length": 1024}},
            size=1024,
        )
        zmp_path = builder.write(tmp_path / "out.zmp")

        manifest = Manifest(str(zmp_path))
        assert manifest.get_data("native/c/0") is not None
        resolve = json.loads(manifest.get_entry("virtual/c/0").resolve)
        assert resolve["http"]["url"] == "https://example.com/data.bin"

    def test_archive_metadata(self, tmp_path: Path) -> None:
        builder = Builder()
        builder.set_archive_metadata({
            "description": "CT scan",
            "modality": "CT",
            "series_uid": "1.2.840.113619.1234",
        })
        builder.add("zarr.json", text='{}')
        zmp_path = builder.write(tmp_path / "out.zmp")

        manifest = Manifest(str(zmp_path))
        am = manifest.archive_metadata
        assert am is not None
        assert am["description"] == "CT scan"
        assert am["modality"] == "CT"

    def test_per_entry_metadata(self, tmp_path: Path) -> None:
        """Per-entry metadata is stored as JSON and queryable."""
        builder = Builder()
        builder.add("zarr.json", text='{}')
        builder.add(
            "vol/c/0", data=b"\x00" * 16,
            metadata={"SliceLocation": 42.5, "InstanceNumber": 1},
        )
        builder.add(
            "vol/c/1",
            resolve={"http": {"url": "s3://bucket/file.dcm", "offset": 1024, "length": 4096}},
            size=4096,
            metadata={"SliceLocation": 45.0, "InstanceNumber": 2},
        )
        zmp_path = builder.write(tmp_path / "out.zmp")

        manifest = Manifest(str(zmp_path))
        m0 = manifest.get_metadata(path="vol/c/0")
        assert m0["SliceLocation"] == 42.5
        assert m0["InstanceNumber"] == 1
        m1 = manifest.get_metadata(path="vol/c/1")
        assert m1["SliceLocation"] == 45.0

    def test_auto_size(self, tmp_path: Path) -> None:
        """Size is auto-computed from content."""
        builder = Builder()
        builder.add("a.json", text="hello")
        builder.add("b.bin", data=b"\x00" * 100)
        builder.add("c.bin", resolve={"http": {"url": "s3://x", "offset": 0, "length": 500}}, size=500)
        zmp_path = builder.write(tmp_path / "out.zmp")

        manifest = Manifest(str(zmp_path))
        assert manifest["a.json"].size == 5
        assert manifest["b.bin"].size == 100
        assert manifest["c.bin"].size == 500

    def test_addressing_flags(self, tmp_path: Path) -> None:
        """Addressing column is auto-computed."""
        builder = Builder()
        builder.add("a", text="hello")
        builder.add("b", data=b"\x00")
        builder.add("c", resolve={"http": {"url": "s3://x", "offset": 0, "length": 10}}, size=10)
        zmp_path = builder.write(tmp_path / "out.zmp")

        pf = pq.ParquetFile(str(zmp_path))
        table = pf.read()
        addr = {
            table.column("path")[i].as_py(): table.column("addressing")[i].as_py()
            for i in range(len(table))
        }
        assert set(addr["a"]) == {"T"}
        assert set(addr["b"]) == {"D"}
        assert set(addr["c"]) == {"R"}

    def test_mount_addressing_flags(self, tmp_path: Path) -> None:
        """Mount entries get MOUNT, FOLDER, RESOLVE flags."""
        builder = Builder()
        builder.add("zarr.json", text='{}')
        builder.mount("sub", resolve={"http": {"url": "child.zmp"}})
        zmp_path = builder.write(tmp_path / "out.zmp")

        manifest = Manifest(str(zmp_path))
        entry = manifest.get_entry("sub")
        assert entry is not None
        assert Addressing.MOUNT in entry.addressing
        assert Addressing.FOLDER in entry.addressing
        assert Addressing.RESOLVE in entry.addressing

    def test_checksum_present(self, tmp_path: Path) -> None:
        """All content entries have a checksum."""
        builder = Builder()
        builder.add("meta.json", text='{"key": "value"}')
        builder.add("chunk", data=b"\x00" * 16)
        zmp_path = builder.write(tmp_path / "out.zmp")

        manifest = Manifest(str(zmp_path))
        for path in manifest.list_paths():
            if path == "":
                continue
            entry = manifest.get_entry(path)
            assert entry is not None
            assert entry.checksum is not None
            assert len(entry.checksum) == 40

    def test_canonical_json_hashing(self, tmp_path: Path) -> None:
        """Checksums use canonical JSON, not raw input."""
        text = '{"b": 1, "a": 2}'
        builder = Builder()
        builder.add("test.json", text=text)
        zmp_path = builder.write(tmp_path / "out.zmp")

        manifest = Manifest(str(zmp_path))
        entry = manifest.get_entry("test.json")

        canonical = rfc8785.dumps(json.loads(text))
        expected = git_blob_hash(canonical)
        assert entry.checksum == expected
