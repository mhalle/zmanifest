"""Tests for the zmp CLI."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest
from click.testing import CliRunner

from zmanifest import Builder
from zmanifest.cli import cli


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def sample_zmp(tmp_path: Path) -> Path:
    """A small .zmp with text, data, and archive metadata."""
    builder = Builder()
    builder.set_archive_metadata({"description": "test archive", "modality": "CT"})
    builder.add("/zarr.json", text='{"zarr_format":3,"node_type":"group"}')
    builder.add("/arr/zarr.json", text='{"zarr_format":3,"node_type":"array"}')
    chunk = np.array([1.0, 2.0, 3.0, 4.0], dtype="<f8").tobytes()
    builder.add("/arr/c/0", data=chunk)
    builder.add("/arr/c/1", data=chunk)
    builder.add(
        "/virtual/c/0",
        url="https://example.com/data.bin",
        offset=0,
        length=4096,
    )
    return builder.write(tmp_path / "sample.zmp")


class TestInfo:
    def test_info(self, runner: CliRunner, sample_zmp: Path) -> None:
        result = runner.invoke(cli, ["info", str(sample_zmp)])
        assert result.exit_code == 0
        assert "Version:" in result.output
        assert "Entries:" in result.output
        assert "text" in result.output

    def test_info_json(self, runner: CliRunner, sample_zmp: Path) -> None:
        result = runner.invoke(cli, ["info", "--json", str(sample_zmp)])
        assert result.exit_code == 0
        obj = json.loads(result.output)
        assert "entries" in obj
        assert "counts" in obj
        assert obj["zarr_format"] == "3"


class TestList:
    def test_list_default(self, runner: CliRunner, sample_zmp: Path) -> None:
        result = runner.invoke(cli, ["list", str(sample_zmp)])
        assert result.exit_code == 0
        lines = result.output.strip().split("\n")
        assert any("/zarr.json" in l for l in lines)
        assert any("/arr/c/0" in l for l in lines)
        # Archive row should not appear
        assert not any(l.strip() == "" for l in lines if l.strip())

    def test_list_long(self, runner: CliRunner, sample_zmp: Path) -> None:
        result = runner.invoke(cli, ["list", "-l", str(sample_zmp)])
        assert result.exit_code == 0
        # Should have addressing flags and sizes
        assert "T" in result.output or "D" in result.output

    def test_list_prefix(self, runner: CliRunner, sample_zmp: Path) -> None:
        result = runner.invoke(cli, ["list", "-p", "/arr/c", str(sample_zmp)])
        assert result.exit_code == 0
        lines = [l for l in result.output.strip().split("\n") if l.strip()]
        assert len(lines) == 2
        assert all("/arr/c/" in l for l in lines)

    def test_list_json(self, runner: CliRunner, sample_zmp: Path) -> None:
        result = runner.invoke(cli, ["list", "--json", str(sample_zmp)])
        assert result.exit_code == 0
        entries = json.loads(result.output)
        assert isinstance(entries, list)
        assert len(entries) >= 4
        assert all("path" in e for e in entries)


class TestCat:
    def test_cat_text(self, runner: CliRunner, sample_zmp: Path) -> None:
        result = runner.invoke(cli, ["cat", str(sample_zmp), "/zarr.json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["zarr_format"] == 3

    def test_cat_binary(self, runner: CliRunner, sample_zmp: Path) -> None:
        result = runner.invoke(cli, ["cat", str(sample_zmp), "/arr/c/0"])
        assert result.exit_code == 0
        data = result.output_bytes
        arr = np.frombuffer(data, dtype="<f8")
        np.testing.assert_array_equal(arr, [1.0, 2.0, 3.0, 4.0])

    def test_cat_missing(self, runner: CliRunner, sample_zmp: Path) -> None:
        result = runner.invoke(cli, ["cat", str(sample_zmp), "/nonexistent"])
        assert result.exit_code != 0
        assert "not found" in result.output.lower()

    def test_cat_no_inline(self, runner: CliRunner, sample_zmp: Path) -> None:
        result = runner.invoke(cli, ["cat", str(sample_zmp), "/virtual/c/0"])
        assert result.exit_code != 0
        assert "No inline content" in result.output


class TestMetadata:
    def test_archive_metadata(self, runner: CliRunner, sample_zmp: Path) -> None:
        result = runner.invoke(cli, ["metadata", str(sample_zmp)])
        assert result.exit_code == 0
        meta = json.loads(result.output)
        assert meta["description"] == "test archive"
        assert meta["modality"] == "CT"

    def test_no_path_metadata(self, runner: CliRunner, sample_zmp: Path) -> None:
        result = runner.invoke(cli, ["metadata", str(sample_zmp), "/arr/c/0"])
        assert result.exit_code == 0
        assert result.output.strip() == "null"


class TestValidate:
    def test_validate_ok(self, runner: CliRunner, sample_zmp: Path) -> None:
        result = runner.invoke(cli, ["-v", "validate", str(sample_zmp)])
        assert result.exit_code == 0
        assert "OK" in result.output
        assert "FAIL" not in result.output.split("---")[0]

    def test_validate_quiet(self, runner: CliRunner, sample_zmp: Path) -> None:
        result = runner.invoke(cli, ["validate", str(sample_zmp)])
        assert result.exit_code == 0
        assert "OK" in result.output


class TestGet:
    def test_get_to_file(self, runner: CliRunner, sample_zmp: Path, tmp_path: Path) -> None:
        out = tmp_path / "output.json"
        result = runner.invoke(cli, ["get", str(sample_zmp), "/zarr.json", "-o", str(out)])
        assert result.exit_code == 0
        assert out.exists()
        parsed = json.loads(out.read_text())
        assert parsed["zarr_format"] == 3

    def test_get_missing(self, runner: CliRunner, sample_zmp: Path, tmp_path: Path) -> None:
        out = tmp_path / "output.bin"
        result = runner.invoke(cli, ["get", str(sample_zmp), "/nope", "-o", str(out)])
        assert result.exit_code != 0


class TestExtract:
    def test_extract_all(self, runner: CliRunner, sample_zmp: Path, tmp_path: Path) -> None:
        out = tmp_path / "extracted"
        result = runner.invoke(cli, ["extract", str(sample_zmp), "-o", str(out)])
        assert result.exit_code == 0
        assert (out / "zarr.json").exists()
        assert (out / "arr" / "c" / "0").exists()
        assert (out / "arr" / "c" / "1").exists()
        # Virtual entries should not be extracted
        assert not (out / "virtual" / "c" / "0").exists()

    def test_extract_prefix(self, runner: CliRunner, sample_zmp: Path, tmp_path: Path) -> None:
        out = tmp_path / "extracted"
        result = runner.invoke(cli, ["extract", str(sample_zmp), "-o", str(out), "-p", "/arr/c"])
        assert result.exit_code == 0
        assert (out / "arr" / "c" / "0").exists()
        assert not (out / "zarr.json").exists()


class TestCreate:
    def test_create_from_files(self, runner: CliRunner, tmp_path: Path) -> None:
        # Create source files
        src = tmp_path / "src"
        src.mkdir()
        (src / "zarr.json").write_text('{"zarr_format":3}')
        (src / "data.bin").write_bytes(b"\x00" * 100)

        out = tmp_path / "created.zmp"
        result = runner.invoke(cli, ["create", str(out), str(src), "--base", str(src)])
        assert result.exit_code == 0
        assert out.exists()

        from zmanifest import Manifest
        m = Manifest(str(out))
        assert m.has("/zarr.json")
        assert m.has("/data.bin")

    def test_create_from_directory(self, runner: CliRunner, tmp_path: Path) -> None:
        src = tmp_path / "src"
        (src / "arr").mkdir(parents=True)
        (src / "zarr.json").write_text('{"zarr_format":3}')
        (src / "arr" / "c0").write_bytes(b"\x01" * 50)

        out = tmp_path / "created.zmp"
        result = runner.invoke(cli, ["create", str(out), str(src), "--base", str(src)])
        assert result.exit_code == 0

        from zmanifest import Manifest
        m = Manifest(str(out))
        assert m.has("/zarr.json")
        assert m.has("/arr/c0")


class TestConvertCommands:
    def test_dehydrate(self, runner: CliRunner, sample_zmp: Path, tmp_path: Path) -> None:
        out = tmp_path / "dehydrated.zmp"
        result = runner.invoke(cli, ["dehydrate", str(sample_zmp), str(out)])
        assert result.exit_code == 0
        assert out.exists()

    def test_version(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "zmanifest" in result.output.lower() or "version" in result.output.lower()
