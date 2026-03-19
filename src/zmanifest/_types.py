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
    external_uri: str | None = None,
) -> list[Addressing]:
    """Compute addressing flags from populated fields."""
    flags: list[Addressing] = []
    if text is not None:
        flags.append(Addressing.TEXT)
    if data is not None:
        flags.append(Addressing.DATA)
    if retrieval_key is not None:
        flags.append(Addressing.KEY)
    if external_uri is not None:
        flags.append(Addressing.URI)
    return flags


class Addressing(enum.StrEnum):
    """Addressing flags indicating how an entry's content can be resolved."""

    TEXT = "T"
    DATA = "D"
    KEY = "K"
    URI = "U"
    MOUNT = "M"
