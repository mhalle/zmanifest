# Changelog

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
