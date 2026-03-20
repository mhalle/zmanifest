from __future__ import annotations

import enum
from typing import Any, TypedDict


class ManifestMetadata(TypedDict, total=False):
    zmp_version: str
    zarr_format: str
    retrieval_scheme: str
    # Open dictionary — any additional keys
    extra: dict[str, Any]


def compute_addressing(
    *,
    text: str | None = None,
    data: bytes | None = None,
    data_z: bytes | None = None,
    resolve: dict | str | None = None,
    is_link: bool = False,
    is_mount: bool = False,
    is_index: bool = False,
) -> str:
    """Compute addressing flags string from populated fields."""
    flags = ""
    if text is not None:
        flags += Addressing.TEXT
    if data is not None:
        flags += Addressing.DATA
    if data_z is not None:
        flags += Addressing.DATA_Z
    if resolve is not None and not is_link:
        flags += Addressing.RESOLVE
    if is_link:
        flags += Addressing.LINK
    if is_mount:
        flags += Addressing.MOUNT
    if is_index:
        flags += Addressing.INDEX
    return flags


class Addressing(enum.StrEnum):
    """Addressing flags indicating how an entry's content can be resolved."""

    TEXT = "T"
    DATA = "D"
    DATA_Z = "Z"
    RESOLVE = "R"
    LINK = "L"
    MOUNT = "M"
    INDEX = "I"
