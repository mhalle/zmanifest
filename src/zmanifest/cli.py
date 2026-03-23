"""zmp — command-line interface for ZMP manifest files."""

from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

import click

from ._types import Addressing
from .builder import Builder, git_blob_hash
from .manifest import Manifest, ManifestEntry
from .resolve import _decode_content


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _open_manifest(path: str) -> Manifest:
    try:
        return Manifest(path)
    except Exception as e:
        raise click.ClickException(f"Cannot open {path}: {e}")


def _format_size(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    for unit in ("KB", "MB", "GB", "TB"):
        n /= 1024
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
    return f"{n:.1f} PB"


def _entry_to_dict(entry: ManifestEntry) -> dict[str, Any]:
    d = asdict(entry)
    return {k: v for k, v in d.items() if v is not None and v != "" and v != 0}


def _resolve_content(manifest: Manifest, entry: ManifestEntry) -> bytes | None:
    """Get bytes from an inline entry, handling content_encoding."""
    if Addressing.TEXT in entry.addressing and entry.text is not None:
        return entry.text.encode("utf-8")
    if Addressing.DATA in entry.addressing or Addressing.DATA_Z in entry.addressing:
        data = manifest.get_data(entry.path)
        if data is not None:
            return _decode_content(data, entry.content_encoding)
    return None


def _addressing_summary(flags: str) -> str:
    """Short human label for addressing flags."""
    if Addressing.TEXT in flags:
        return "text"
    if Addressing.DATA in flags or Addressing.DATA_Z in flags:
        return "data"
    if Addressing.RESOLVE in flags:
        return "ref"
    if Addressing.MOUNT in flags:
        return "mount"
    if Addressing.LINK in flags:
        return "link"
    if Addressing.FOLDER in flags:
        return "folder"
    return flags or "?"


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------


@click.group()
@click.version_option(package_name="zmanifest")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output.")
@click.option("--quiet", "-q", is_flag=True, help="Suppress status messages.")
@click.pass_context
def cli(ctx: click.Context, verbose: bool, quiet: bool) -> None:
    """ZMP archive tool — inspect, create, and manipulate .zmp manifest files."""
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["quiet"] = quiet


# ---------------------------------------------------------------------------
# info
# ---------------------------------------------------------------------------


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.option("--json", "as_json", is_flag=True, help="JSON output.")
@click.pass_context
def info(ctx: click.Context, file: str, as_json: bool) -> None:
    """Show archive summary."""
    m = _open_manifest(file)
    meta = m.metadata

    counts: dict[str, int] = {"text": 0, "data": 0, "ref": 0, "mount": 0, "link": 0, "folder": 0}
    total_size = 0

    for p in m.list_paths():
        if p == "":
            continue
        entry = m.get_entry(p)
        if entry is None:
            continue
        total_size += entry.size
        label = _addressing_summary(entry.addressing)
        counts[label] = counts.get(label, 0) + 1

    am = m.archive_metadata

    if as_json:
        obj: dict[str, Any] = {
            "file": file,
            "zmp_version": meta.get("zmp_version", ""),
            "zarr_format": meta.get("zarr_format", ""),
            "entries": len(m) - 1,  # exclude archive row
            "total_size": total_size,
            "counts": {k: v for k, v in counts.items() if v > 0},
        }
        if am:
            obj["archive_metadata"] = am
        click.echo(json.dumps(obj, indent=2))
    else:
        click.echo(f"File:         {file}")
        click.echo(f"Version:      {meta.get('zmp_version', '?')}")
        click.echo(f"Zarr format:  {meta.get('zarr_format', '?')}")
        parts = [f"{v} {k}" for k, v in counts.items() if v > 0]
        click.echo(f"Entries:      {len(m) - 1} ({', '.join(parts)})")
        click.echo(f"Total size:   {_format_size(total_size)}")
        if am:
            click.echo("Archive metadata:")
            for k, v in am.items():
                click.echo(f"  {k}: {v}")


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


@cli.command("list")
@click.argument("file", type=click.Path(exists=True))
@click.option("--long", "-l", is_flag=True, help="Show size, addressing, checksum.")
@click.option("--prefix", "-p", default=None, help="Filter to paths under prefix.")
@click.option("--json", "as_json", is_flag=True, help="JSON output.")
@click.pass_context
def list_cmd(ctx: click.Context, file: str, long: bool, prefix: str | None, as_json: bool) -> None:
    """List archive contents."""
    m = _open_manifest(file)

    paths = list(m.list_prefix(prefix)) if prefix else list(m.list_paths())

    if as_json:
        entries = []
        for p in paths:
            if p == "":
                continue
            entry = m.get_entry(p)
            if entry:
                entries.append(_entry_to_dict(entry))
        click.echo(json.dumps(entries, indent=2))
    elif long:
        for p in paths:
            if p == "":
                continue
            entry = m.get_entry(p)
            if entry is None:
                continue
            flags = entry.addressing or "-"
            cksum = (entry.checksum or "")[:12]
            click.echo(f"{flags:<4} {entry.size:>10}  {cksum:<12}  {entry.path}")
    else:
        for p in paths:
            if p == "":
                continue
            click.echo(p)


# ---------------------------------------------------------------------------
# show
# ---------------------------------------------------------------------------


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.argument("path")
@click.pass_context
def show(ctx: click.Context, file: str, path: str) -> None:
    """Show full detail for a single entry as JSON."""
    m = _open_manifest(file)
    entry = m.get_entry(path)
    if entry is None:
        raise click.ClickException(f"Path not found: {path}")
    click.echo(json.dumps(_entry_to_dict(entry), indent=2))


# ---------------------------------------------------------------------------
# cat
# ---------------------------------------------------------------------------


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.argument("path")
@click.pass_context
def cat(ctx: click.Context, file: str, path: str) -> None:
    """Print entry content to stdout."""
    m = _open_manifest(file)
    entry = m.get_entry(path)
    if entry is None:
        raise click.ClickException(f"Path not found: {path}")

    content = _resolve_content(m, entry)
    if content is None:
        raise click.ClickException(
            f"No inline content for {path} (addressing: {entry.addressing})"
        )

    if Addressing.TEXT in entry.addressing:
        click.echo(content.decode("utf-8"), nl=True)
    else:
        sys.stdout.buffer.write(content)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.argument("path", default=None, required=False)
@click.pass_context
def metadata(ctx: click.Context, file: str, path: str | None) -> None:
    """Show archive or path metadata as JSON."""
    m = _open_manifest(file)

    if path is None:
        meta = m.archive_metadata
    else:
        meta = m.path_metadata(path)
        if meta is None:
            meta = m.get_metadata(path=path)

    if meta is None:
        click.echo("null")
    else:
        click.echo(json.dumps(meta, indent=2))


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.option("--prefix", "-p", default=None, help="Only validate paths under prefix.")
@click.pass_context
def validate(ctx: click.Context, file: str, prefix: str | None) -> None:
    """Verify checksums of inline entries."""
    m = _open_manifest(file)
    verbose = ctx.obj.get("verbose", False)

    paths = list(m.list_prefix(prefix)) if prefix else list(m.list_paths())

    ok = 0
    fail = 0
    skip = 0

    for p in paths:
        if p == "":
            continue
        entry = m.get_entry(p)
        if entry is None:
            continue

        content = _resolve_content(m, entry)
        if content is None or entry.checksum is None:
            skip += 1
            continue

        actual = git_blob_hash(content)
        if actual == entry.checksum:
            ok += 1
            if verbose:
                click.echo(f"OK   {p}")
        else:
            fail += 1
            click.echo(f"FAIL {p}  expected={entry.checksum[:12]}  got={actual[:12]}")

    click.echo(f"---\n{ok} OK, {fail} FAILED, {skip} skipped")
    if fail > 0:
        ctx.exit(1)


# ---------------------------------------------------------------------------
# get
# ---------------------------------------------------------------------------


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.argument("path")
@click.option("-o", "--output", required=True, type=click.Path(), help="Output file path.")
@click.pass_context
def get(ctx: click.Context, file: str, path: str, output: str) -> None:
    """Extract a single entry to a file."""
    m = _open_manifest(file)
    entry = m.get_entry(path)
    if entry is None:
        raise click.ClickException(f"Path not found: {path}")

    content = _resolve_content(m, entry)
    if content is None:
        raise click.ClickException(
            f"No inline content for {path} (addressing: {entry.addressing})"
        )

    out = Path(output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(content)

    if not ctx.obj.get("quiet"):
        click.echo(f"Wrote {len(content)} bytes to {output}", err=True)


# ---------------------------------------------------------------------------
# extract
# ---------------------------------------------------------------------------


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.option("-o", "--output", required=True, type=click.Path(), help="Output directory.")
@click.option("--prefix", "-p", default=None, help="Only extract paths under prefix.")
@click.pass_context
def extract(ctx: click.Context, file: str, output: str, prefix: str | None) -> None:
    """Extract all inline entries to a directory."""
    m = _open_manifest(file)
    verbose = ctx.obj.get("verbose", False)
    quiet = ctx.obj.get("quiet", False)

    out_dir = Path(output)
    out_dir.mkdir(parents=True, exist_ok=True)

    paths = list(m.list_prefix(prefix)) if prefix else list(m.list_paths())
    count = 0

    for p in paths:
        if p == "":
            continue
        entry = m.get_entry(p)
        if entry is None:
            continue

        content = _resolve_content(m, entry)
        if content is None:
            continue

        # Strip leading / for filesystem path
        rel = p.lstrip("/")
        dest = out_dir / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)
        count += 1

        if verbose:
            click.echo(f"  {p} ({len(content)} bytes)", err=True)

    if not quiet:
        click.echo(f"Extracted {count} entries to {output}", err=True)


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------


@cli.command()
@click.argument("output", type=click.Path())
@click.argument("files", nargs=-1, required=True, type=click.Path(exists=True))
@click.option("--base", "-b", default=None, type=click.Path(exists=True),
              help="Base directory (paths are relative to this).")
@click.option("--zarr-format", default="3", help="Zarr format version.")
@click.option("--compress", default=None, help="Compress inline data (deflate, zstd, etc.).")
@click.pass_context
def create(ctx: click.Context, output: str, files: tuple[str, ...],
           base: str | None, zarr_format: str, compress: str | None) -> None:
    """Create a .zmp from files on disk."""
    quiet = ctx.obj.get("quiet", False)
    verbose = ctx.obj.get("verbose", False)

    base_path = Path(base).resolve() if base else None
    builder = Builder(zarr_format=zarr_format)

    count = 0
    for f in files:
        fp = Path(f)
        if fp.is_dir():
            items = sorted(fp.rglob("*"))
        else:
            items = [fp]

        for item in items:
            if item.is_dir():
                continue

            if base_path:
                try:
                    rel = item.resolve().relative_to(base_path)
                except ValueError:
                    rel = item
                archive_path = "/" + str(rel)
            else:
                archive_path = "/" + str(item)

            raw = item.read_bytes()

            if item.suffix == ".json":
                try:
                    text = raw.decode("utf-8")
                    builder.add(archive_path, text=text)
                except UnicodeDecodeError:
                    builder.add(archive_path, data=raw, compress=compress)
            else:
                builder.add(archive_path, data=raw, compress=compress)

            count += 1
            if verbose:
                click.echo(f"  {archive_path} ({len(raw)} bytes)", err=True)

    result = builder.write(output)
    if not quiet:
        click.echo(f"Created {result} ({count} entries)", err=True)


# ---------------------------------------------------------------------------
# import-zip
# ---------------------------------------------------------------------------


# Map zipfile compression methods to content_encoding values
_ZIP_ENCODINGS = {
    0: None,        # ZIP_STORED — no compression
    8: "deflate",   # ZIP_DEFLATED
    12: "bz2",      # ZIP_BZIP2
    14: "lzma",     # ZIP_LZMA
}


def _zip_data_offset(info: Any) -> int:
    """Compute the byte offset of the compressed data within a zip file.

    The local file header is 30 bytes + filename length + extra field length,
    followed by the compressed data.
    """
    return info.header_offset + 30 + len(info.filename.encode("utf-8")) + len(info.extra)


@cli.command("import-zip")
@click.argument("zipfile_path")
@click.argument("output", type=click.Path())
@click.option("--virtual", is_flag=True,
              help="Keep data in the zip file (store offset/length references).")
@click.option("--zarr-format", default="3", help="Zarr format version.")
@click.option("--prefix", "-p", default=None,
              help="Only import entries under this prefix within the zip.")
@click.pass_context
def import_zip(ctx: click.Context, zipfile_path: str, output: str,
               virtual: bool, zarr_format: str, prefix: str | None) -> None:
    """Import entries from a zip file or URL into a .zmp manifest.

    By default, extracts and inlines all zip entries. With --virtual,
    stores byte-range references back into the zip file instead, using
    base_resolve to avoid repeating the zip path/URL in every entry.

    Works with local files, HTTP(S) URLs, and .zarr.zip archives.
    Remote URLs use HTTP range requests (no full download needed).
    """
    import zipfile as zf

    verbose = ctx.obj.get("verbose", False)
    quiet = ctx.obj.get("quiet", False)

    is_remote = zipfile_path.startswith(("http://", "https://"))

    if is_remote:
        resolve_url = zipfile_path
    else:
        local_path = Path(zipfile_path)
        if not local_path.exists():
            raise click.ClickException(f"File not found: {zipfile_path}")
        resolve_url = str(local_path.resolve())

    if virtual:
        builder = Builder(
            zarr_format=zarr_format,
            base_resolve={"http": {"url": resolve_url}},
        )
    else:
        builder = Builder(zarr_format=zarr_format)

    # Open zip — remotezip for URLs (range requests), zipfile for local
    if is_remote:
        import remotezip
        try:
            archive_cm = remotezip.RemoteZip(zipfile_path)
        except remotezip.RemoteIOError as e:
            raise click.ClickException(f"Cannot open remote zip: {e}")
    else:
        archive_cm = zf.ZipFile(str(local_path), "r")

    count = 0
    with archive_cm as archive:
        for info in archive.infolist():
            # Skip directories
            if info.is_dir():
                continue

            name = info.filename
            if prefix and not name.startswith(prefix):
                continue

            archive_path = "/" + name

            # Metadata files are always inlined as text (small, needed
            # for zarr structure discovery). Covers zarr v3 (.json) and
            # zarr v2 (.zarray, .zgroup, .zattrs, .zmetadata).
            basename = name.rsplit("/", 1)[-1] if "/" in name else name
            _TEXT_NAMES = {".zarray", ".zgroup", ".zattrs", ".zmetadata"}
            if name.endswith(".json") or basename in _TEXT_NAMES:
                data = archive.read(name)
                try:
                    builder.add(archive_path, text=data.decode("utf-8"))
                except UnicodeDecodeError:
                    builder.add(archive_path, data=data)
                count += 1
                if verbose:
                    click.echo(f"  {archive_path} ({len(data)} bytes, text)", err=True)
                continue

            if virtual:
                # Store reference into the zip file
                encoding = _ZIP_ENCODINGS.get(info.compress_type)
                if encoding is None and info.compress_type != 0:
                    # Unknown compression — fall back to inline
                    builder.add(archive_path, data=archive.read(name))
                elif info.compress_type == 0:
                    # Stored (no compression) — reference with no encoding
                    data_offset = _zip_data_offset(info)
                    builder.add(
                        archive_path,
                        resolve={"http": {"offset": data_offset, "length": info.file_size}},
                        size=info.file_size,
                    )
                else:
                    # Compressed — reference with content_encoding
                    data_offset = _zip_data_offset(info)
                    builder.add(
                        archive_path,
                        resolve={"http": {"offset": data_offset, "length": info.compress_size}},
                        content_encoding=encoding,
                        size=info.file_size,
                    )
            else:
                # Inline mode — extract and store
                builder.add(archive_path, data=archive.read(name))

            count += 1
            if verbose:
                mode = "ref" if virtual else "inline"
                click.echo(f"  {archive_path} ({info.file_size} bytes, {mode})", err=True)

    result = builder.write(output)
    if not quiet:
        mode = "virtual" if virtual else "inline"
        click.echo(f"Imported {count} entries from {zipfile_path} ({mode}) to {result}", err=True)


# ---------------------------------------------------------------------------
# hash / dehydrate / hydrate
# ---------------------------------------------------------------------------


@cli.command("hash")
@click.argument("input", type=click.Path(exists=True))
@click.argument("output", type=click.Path())
@click.pass_context
def hash_cmd(ctx: click.Context, input: str, output: str) -> None:
    """Compute missing checksums."""
    from . import convert
    result = convert.hash(input, output)
    if not ctx.obj.get("quiet"):
        click.echo(f"Wrote {result}", err=True)


@cli.command()
@click.argument("input", type=click.Path(exists=True))
@click.argument("output", type=click.Path())
@click.option("--chunk-dir", default=None, type=click.Path(),
              help="Write stripped data blobs to this directory.")
@click.pass_context
def dehydrate(ctx: click.Context, input: str, output: str, chunk_dir: str | None) -> None:
    """Strip inline data, keep references."""
    from . import convert
    result = convert.dehydrate(input, output, chunk_dir=chunk_dir)
    if not ctx.obj.get("quiet"):
        click.echo(f"Wrote {result}", err=True)


@cli.command()
@click.argument("input", type=click.Path(exists=True))
@click.argument("output", type=click.Path())
@click.option("--chunk-dir", default=None, type=click.Path(exists=True),
              help="Directory with data blobs keyed by checksum.")
@click.option("--prefix", default=None, help="Only hydrate paths under prefix.")
@click.pass_context
def hydrate(ctx: click.Context, input: str, output: str,
            chunk_dir: str | None, prefix: str | None) -> None:
    """Resolve references and inline data."""
    from . import convert

    if chunk_dir is None:
        raise click.ClickException("--chunk-dir is required for hydrate")

    chunk_path = Path(chunk_dir)

    class _FileResolver:
        async def resolve(self, key: str) -> bytes | None:
            p = chunk_path / key
            if p.exists():
                return p.read_bytes()
            return None

    result = convert.hydrate(input, output, _FileResolver(), prefix=prefix)
    if not ctx.obj.get("quiet"):
        click.echo(f"Wrote {result}", err=True)
