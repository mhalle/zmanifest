"""Tests for ZPath."""

from zmanifest.path import ZPath


class TestConstruction:
    def test_root(self) -> None:
        assert ZPath("/") == ZPath.ROOT
        assert ZPath("") == ZPath.ROOT
        assert ZPath().is_root

    def test_absolute(self) -> None:
        p = ZPath("/a/b/c")
        assert str(p) == "/a/b/c"

    def test_auto_prefix(self) -> None:
        p = ZPath("a/b")
        assert str(p) == "/a/b"

    def test_normalize_trailing(self) -> None:
        assert str(ZPath("/a/b/")) == "/a/b"

    def test_normalize_double_slash(self) -> None:
        assert str(ZPath("/a//b")) == "/a/b"

    def test_from_zarr_empty(self) -> None:
        assert ZPath.from_zarr("") == ZPath.ROOT

    def test_from_zarr_path(self) -> None:
        assert ZPath.from_zarr("scans/ct/c/0") == ZPath("/scans/ct/c/0")


class TestConversion:
    def test_to_zarr_root(self) -> None:
        assert ZPath.ROOT.to_zarr() == ""

    def test_to_zarr_path(self) -> None:
        assert ZPath("/scans/ct/c/0").to_zarr() == "scans/ct/c/0"

    def test_roundtrip(self) -> None:
        for zarr_path in ["", "a", "a/b/c", "group/arr/c/0/1"]:
            assert ZPath.from_zarr(zarr_path).to_zarr() == zarr_path


class TestProperties:
    def test_name_root(self) -> None:
        assert ZPath.ROOT.name == ""

    def test_name(self) -> None:
        assert ZPath("/a/b/c").name == "c"
        assert ZPath("/a").name == "a"

    def test_parent_root(self) -> None:
        assert ZPath.ROOT.parent == ZPath.ROOT

    def test_parent(self) -> None:
        assert ZPath("/a/b/c").parent == ZPath("/a/b")
        assert ZPath("/a").parent == ZPath.ROOT

    def test_parts_root(self) -> None:
        assert ZPath.ROOT.parts == ()

    def test_parts(self) -> None:
        assert ZPath("/a/b/c").parts == ("a", "b", "c")

    def test_depth(self) -> None:
        assert ZPath.ROOT.depth == 0
        assert ZPath("/a").depth == 1
        assert ZPath("/a/b/c").depth == 3

    def test_bool(self) -> None:
        assert not ZPath.ROOT
        assert ZPath("/a")


class TestJoin:
    def test_join_from_root(self) -> None:
        assert ZPath.ROOT / "a" == ZPath("/a")

    def test_join_child(self) -> None:
        assert ZPath("/a/b") / "c" == ZPath("/a/b/c")

    def test_join_strips_slashes(self) -> None:
        assert ZPath("/a") / "/b/" == ZPath("/a/b")

    def test_join_empty(self) -> None:
        assert ZPath("/a") / "" == ZPath("/a")

    def test_join_multi(self) -> None:
        assert ZPath("/a") / "b/c" == ZPath("/a/b/c")


class TestPrefixOps:
    def test_is_child_of_root(self) -> None:
        assert ZPath("/a").is_child_of("/")
        assert not ZPath.ROOT.is_child_of("/")

    def test_is_child_of(self) -> None:
        assert ZPath("/a/b/c").is_child_of("/a")
        assert ZPath("/a/b/c").is_child_of("/a/b")
        assert not ZPath("/a/b/c").is_child_of("/a/b/c")
        assert not ZPath("/abc").is_child_of("/a")  # not prefix match

    def test_is_equal_or_child_of(self) -> None:
        assert ZPath("/a/b").is_equal_or_child_of("/a/b")
        assert ZPath("/a/b/c").is_equal_or_child_of("/a/b")
        assert not ZPath("/a").is_equal_or_child_of("/a/b")

    def test_relative_to_root(self) -> None:
        assert ZPath("/a/b").relative_to("/") == "a/b"
        assert ZPath.ROOT.relative_to("/") == ""

    def test_relative_to(self) -> None:
        assert ZPath("/a/b/c").relative_to("/a") == "b/c"
        assert ZPath("/a/b/c").relative_to("/a/b") == "c"

    def test_relative_to_error(self) -> None:
        import pytest
        with pytest.raises(ValueError):
            ZPath("/a/b").relative_to("/x")

    def test_child_name_under(self) -> None:
        assert ZPath("/a/b/c").child_name_under("/a") == "b"
        assert ZPath("/a/b").child_name_under("/a") == "b"
        assert ZPath("/a").child_name_under("/a") is None
        assert ZPath("/x").child_name_under("/a") is None

    def test_immediate_children(self) -> None:
        paths = [
            ZPath("/scans/ct/arr/c/0"),
            ZPath("/scans/ct/arr/c/1"),
            ZPath("/scans/ct/arr/zarr.json"),
            ZPath("/scans/mr/arr/c/0"),
            ZPath("/zarr.json"),
        ]
        assert ZPath("/scans").immediate_children(iter(paths)) == {"ct", "mr"}
        assert ZPath("/scans/ct").immediate_children(iter(paths)) == {"arr"}
        assert ZPath.ROOT.immediate_children(iter(paths)) == {"scans", "zarr.json"}


class TestEquality:
    def test_eq_zpath(self) -> None:
        assert ZPath("/a/b") == ZPath("/a/b")
        assert ZPath("/a") != ZPath("/b")

    def test_eq_str(self) -> None:
        assert ZPath("/a/b") == "/a/b"
        assert ZPath("/a/b") == "a/b"  # auto-normalizes

    def test_hash(self) -> None:
        s = {ZPath("/a"), ZPath("/a"), ZPath("/b")}
        assert len(s) == 2

    def test_sorted(self) -> None:
        paths = [ZPath("/c"), ZPath("/a"), ZPath("/b")]
        assert sorted(paths) == [ZPath("/a"), ZPath("/b"), ZPath("/c")]


class TestRepr:
    def test_repr(self) -> None:
        assert repr(ZPath("/a/b")) == "ZPath('/a/b')"
        assert repr(ZPath.ROOT) == "ZPath('/')"
