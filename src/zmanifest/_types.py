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
    retrieval_key: str | None = None,
    uri: str | None = None,
    is_link: bool = False,
) -> list[Addressing]:
    """Compute addressing flags from populated fields."""
    flags: list[Addressing] = []
    if text is not None:
        flags.append(Addressing.TEXT)
    if data is not None:
        flags.append(Addressing.DATA)
    if retrieval_key is not None:
        flags.append(Addressing.KEY)
    if uri is not None and not is_link:
        flags.append(Addressing.URI)
    if is_link:
        flags.append(Addressing.LINK)
    return flags


class Addressing(enum.StrEnum):
    """Addressing flags indicating how an entry's content can be resolved."""

    TEXT = "T"
    DATA = "D"
    KEY = "K"
    URI = "U"
    LINK = "L"
    MOUNT = "M"
