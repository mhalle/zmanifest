# Changelog

## v0.12.0 (2026-03-22)

### Breaking changes

- **Absolute paths on disk**: Manifest paths are now stored with leading `/`
  (e.g. `/arr/c/0` instead of `arr/c/0`). Old files with bare paths are
  read correctly — paths are normalized to absolute on load.
- **Row order changed**: Data rows first, then non-data, then archive row.
  The old index row is no longer written (but old files with one are still
  readable).
- `set_root_metadata()` renamed to `set_archive_metadata()`;
  `root_metadata` property renamed to `archive_metadata`.
  Old names kept as aliases.

### Features

- **ZPath**: New absolute path type (`zmanifest.ZPath`) with pathlib-like
  operators — join (`/`), `parent`, `name`, `parts`, `is_child_of`,
  `relative_to`, `child_name_under`. Replaces raw string manipulation.
  `from_zarr()` / `to_zarr()` for zarr interop.
- **Adaptive row group sizing**: Builder targets ~10 MB of blob data per
  row group (cap 2000 rows) instead of 1-row-per-group. Reduces footer
  overhead by 100–1000x for large archives while keeping blob fetch
  under ~5 ms.
- **Streaming builder**: `Builder(output="file.zmp")` streams data rows
  to disk as they're added instead of buffering everything in memory.
  Non-data rows (small) are buffered and written at `close()`.
- **Per-column compression**: `data` column uncompressed (zarr chunks are
  pre-compressed), all other columns ZSTD.
- **Page indexes**: `write_page_index=True` for page-level skipping.
- **Archive metadata**: `""` row clearly distinguished as archive-level
  metadata (container provenance, DICOM series UID) vs `/` root directory.
- **Test suite**: 66 tests (path, manifest, builder, streaming).
- **Benchmarks**: `benchmarks/` directory with parquet layout experiments.
- **Documentation**: `docs/parquet-layout.md` with full analysis, benchmark
  data, bloom filter assessment, and recommended layout.

### Backward compatibility

- Old manifest files (bare paths, index row) are read correctly.
  Paths are normalized to absolute on load. The `_try_load_index()`
  fallback path handles the old index row format.
- `set_root_metadata` / `root_metadata` still work as aliases.
- `Builder.write()` (batch mode) still works unchanged.

## v0.1.0 (2026-03-19)

Initial release of zmanifest — a content-addressed file manifest format backed by Apache Parquet.

### Features

- **Manifest format**: Parquet-based manifest with per-entry addressing (`T`ext, `D`ata, `K`ey, `U`RI, `L`ink, `M`ount)
- **Builder**: Row-level manifest construction with auto-computed retrieval keys (git-sha1), size inference, and array path parsing
- **Resolvers**: `TemplateResolver` (local/HTTP with `{hash}` templates and slice syntax), `GitResolver` (bare git repos via vost)
- **Content resolution**: Async `resolve_entry()` follows the spec resolution order (text → data → key → link → URI)
- **Link addressing** (`L`): Path aliases within a manifest; resolution follows the target with cycle detection
- **Mount support**: Compose manifests by mounting child `.zmp` files at path prefixes
- **Relative URI resolution**: `uri` values resolve against per-row `base_uri`, file-level metadata, or the manifest's own location
- **Manifest conversion**: `hash()`, `hydrate()`, `dehydrate()` to transform between self-contained and reference-only manifests
- **`canonical_json()`**: RFC 8785 JSON canonicalization helper for deterministic hashing
- **`git_blob_hash()`**: Public git blob SHA-1 function (`SHA-1("blob <size>\0<content>")`)
- **Queryable**: Standard Parquet — works with DuckDB, Polars, Spark out of the box

### Column schema

`path`, `id`, `size`, `content_size`, `retrieval_key`, `text`, `data`, `uri`, `offset`, `length`, `array_path`, `chunk_key`, `media_type`, `source`, `checksum`, `base_uri`, `metadata`, `addressing`
