"""Tests for Manifest reading and querying."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from zmanifest import Manifest


class TestManifest:
    def test_metadata(self, simple_zmp: Path) -> None:
        m = Manifest(str(simple_zmp))
        assert m.metadata["zmp_version"] == "0.2.0"
        assert m.metadata["zarr_format"] == "3"

    def test_has(self, simple_zmp: Path) -> None:
        m = Manifest(str(simple_zmp))
        assert m.has("/zarr.json")
        assert m.has("/temp/c/0")
        assert not m.has("/nonexistent")
        # Bare paths also accepted
        assert m.has("zarr.json")
        assert m.has("temp/c/0")

    def test_get_entry(self, simple_zmp: Path) -> None:
        m = Manifest(str(simple_zmp))
        entry = m.get_entry("/zarr.json")
        assert entry is not None
        assert entry.text is not None
        parsed = json.loads(entry.text)
        assert parsed["node_type"] == "group"
        # Path in entry should be absolute
        assert entry.path == "/zarr.json"

    def test_get_entry_missing(self, simple_zmp: Path) -> None:
        m = Manifest(str(simple_zmp))
        assert m.get_entry("/nonexistent") is None

    def test_get_data(self, simple_zmp: Path) -> None:
        m = Manifest(str(simple_zmp))
        data = m.get_data("/temp/c/0")
        assert data is not None
        arr = np.frombuffer(data, dtype="<f8")
        np.testing.assert_array_equal(arr, [0.0, 1.0])

    def test_list_paths(self, simple_zmp: Path) -> None:
        m = Manifest(str(simple_zmp))
        paths = list(m.list_paths())
        assert "/zarr.json" in paths
        assert "/temp/c/0" in paths

    def test_list_prefix(self, simple_zmp: Path) -> None:
        m = Manifest(str(simple_zmp))
        assert len(list(m.list_prefix("/temp/c"))) == 4
        assert len(list(m.list_prefix("/temp"))) == 5

    def test_list_dir(self, simple_zmp: Path) -> None:
        m = Manifest(str(simple_zmp))
        top = list(m.list_dir("/"))
        assert "zarr.json" in top
        assert "temp/" in top
        assert len(top) == 2

        temp = list(m.list_dir("/temp"))
        assert "zarr.json" in temp
        assert "c/" in temp
