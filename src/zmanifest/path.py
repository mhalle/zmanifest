"""Archive path type for zmanifest.

Paths are absolute (start with ``/``) and use ``/`` as separator.
Root is ``/``. There are no trailing slashes except for root.

    >>> p = ZPath("/scans/ct/arr/c/0")
    >>> p.name
    '0'
    >>> p.parent
    ZPath('/scans/ct/arr/c')
    >>> p / "1"
    ZPath('/scans/ct/arr/c/0/1')
    >>> p.parts
    ('scans', 'ct', 'arr', 'c', '0')
    >>> p.is_child_of("/scans/ct")
    True
    >>> p.relative_to("/scans/ct")
    'arr/c/0'
    >>> ZPath.ROOT
    ZPath('/')

Conversion to/from zarr bare paths (no leading ``/``):

    >>> ZPath.from_zarr("scans/ct/arr/c/0")
    ZPath('/scans/ct/arr/c/0')
    >>> ZPath("/scans/ct/arr/c/0").to_zarr()
    'scans/ct/arr/c/0'
    >>> ZPath.ROOT.to_zarr()
    ''
"""

from __future__ import annotations

from functools import total_ordering
from typing import Iterator


def _normalize(path: str) -> str:
    """Normalize to canonical form: leading ``/``, no trailing ``/``, no ``//``."""
    if not path or path == "/":
        return "/"
    # Ensure leading /
    if not path.startswith("/"):
        path = "/" + path
    # Remove trailing /
    path = path.rstrip("/")
    # Collapse //
    while "//" in path:
        path = path.replace("//", "/")
    return path or "/"


@total_ordering
class ZPath:
    """Immutable, absolute, ``/``-separated archive path."""

    __slots__ = ("_path",)

    ROOT: ZPath  # set after class body

    def __init__(self, path: str = "/") -> None:
        self._path = _normalize(path)

    # -- Constructors ---------------------------------------------------------

    @classmethod
    def from_zarr(cls, zarr_path: str) -> ZPath:
        """Create from a zarr-style bare path (no leading ``/``)."""
        if zarr_path == "" or zarr_path == "/":
            return cls.ROOT
        return cls("/" + zarr_path.lstrip("/"))

    # -- Conversions ----------------------------------------------------------

    def to_zarr(self) -> str:
        """Return the zarr-style bare path (no leading ``/``)."""
        if self._path == "/":
            return ""
        return self._path[1:]

    def __str__(self) -> str:
        return self._path

    def __repr__(self) -> str:
        return f"ZPath({self._path!r})"

    def __hash__(self) -> int:
        return hash(self._path)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ZPath):
            return self._path == other._path
        if isinstance(other, str):
            return self._path == _normalize(other)
        return NotImplemented

    def __lt__(self, other: object) -> bool:
        if isinstance(other, ZPath):
            return self._path < other._path
        return NotImplemented

    def __bool__(self) -> bool:
        """Root is falsy (like empty string), all others truthy."""
        return self._path != "/"

    # -- Path algebra ---------------------------------------------------------

    def __truediv__(self, other: str | ZPath) -> ZPath:
        """Join: ``ZPath("/a") / "b"`` → ``ZPath("/a/b")``."""
        child = str(other).strip("/")
        if not child:
            return self
        if self._path == "/":
            return ZPath("/" + child)
        return ZPath(self._path + "/" + child)

    @property
    def name(self) -> str:
        """Final component, or ``""`` for root."""
        if self._path == "/":
            return ""
        return self._path.rsplit("/", 1)[-1]

    @property
    def parent(self) -> ZPath:
        """Parent path. Root's parent is root."""
        if self._path == "/":
            return self
        head = self._path.rsplit("/", 1)[0]
        return ZPath(head if head else "/")

    @property
    def parts(self) -> tuple[str, ...]:
        """Path components (excluding root ``/``)."""
        if self._path == "/":
            return ()
        return tuple(self._path[1:].split("/"))

    @property
    def is_root(self) -> bool:
        return self._path == "/"

    @property
    def depth(self) -> int:
        """Number of components. Root is 0."""
        if self._path == "/":
            return 0
        return self._path.count("/")

    # -- Prefix operations (for mounts, links, listing) ----------------------

    def is_child_of(self, ancestor: str | ZPath) -> bool:
        """True if this path is strictly under ``ancestor``."""
        a = _normalize(str(ancestor))
        if a == "/":
            return self._path != "/"
        return self._path.startswith(a + "/")

    def is_equal_or_child_of(self, ancestor: str | ZPath) -> bool:
        """True if this path equals or is under ``ancestor``."""
        return self == ancestor or self.is_child_of(ancestor)

    def relative_to(self, ancestor: str | ZPath) -> str:
        """Return the portion after ``ancestor``, without leading ``/``.

        Raises ``ValueError`` if this path is not under ``ancestor``.
        """
        a = _normalize(str(ancestor))
        if a == "/":
            if self._path == "/":
                return ""
            return self._path[1:]
        prefix = a + "/"
        if not self._path.startswith(prefix):
            raise ValueError(f"{self!r} is not under {a!r}")
        return self._path[len(prefix) :]

    def child_name_under(self, ancestor: str | ZPath) -> str | None:
        """Return the immediate child name under ``ancestor``, or None.

        ``ZPath("/a/b/c").child_name_under("/a")`` → ``"b"``
        ``ZPath("/a").child_name_under("/a")`` → ``None``
        """
        try:
            rel = self.relative_to(ancestor)
        except ValueError:
            return None
        if not rel:
            return None
        return rel.split("/", 1)[0]

    def immediate_children(self, paths: Iterator[ZPath]) -> set[str]:
        """From an iterable of paths, collect immediate child names under self."""
        children: set[str] = set()
        for p in paths:
            name = p.child_name_under(self)
            if name is not None:
                children.add(name)
        return children


ZPath.ROOT = ZPath("/")
