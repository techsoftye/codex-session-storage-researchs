# Analysis pipeline

The measurements were produced by a set of Python analyzers that use the standard library only
(Python 3.14 for `compression.zstd`). A PostgreSQL 18 database holds record locators and token
observations. The analyzers themselves are not part of this release, because they run against
private local data.

## Rules every analyzer followed

- **Read-only.** Read-only access to copies of the session files, with size and modification
  time verified before and after each file.
- **Streaming.** Files are read in 256 KiB chunks, one process at below-normal priority, with
  atomic per-file checkpoints.
- **Aggregates only.** Outputs hold counts, byte sizes, hashes, field paths and short
  enumerations, never content.
- **Loud failures.** Parse errors and limits stop a run with an error.
- **Tests.** Synthetic unit tests cover every analyzer, for example chunk boundaries and
  byte-exact reconstruction for the reference variants.

## Stages

| Stage | What it measures | Results used in |
|---|---|---|
| Inventory and record index | File sizes; one locator per physical record: line, offset, length, timestamp, type | Paper §4, §7.2 |
| Structural censuses | Field paths, types and byte sizes of `response_item`, `event_msg` and `compacted` records | §7–§9 |
| Hash indexes | SHA-256 of every embedded image (decoded) and text block in compacted histories | §8.3, §11 |
| Growth and composition | Snapshot growth, carry-forward between consecutive snapshots, and a byte decomposition in which categories do not overlap | §7.3, §9.2 |
| Token observations | Every token-counter object with its locator; duplicate candidates flagged, never deleted | §10 |
| Token case studies | Turn timeline, output components, storage–token association with length control | §10 |
| Before/after | Codex `.zst` files decompressed and compared record by record with pre-change copies | §12 |
| Reference variants | Each file streamed once; images, and optionally history items, replaced by references; every stream compressed with zstd level 3 in memory | §14 |

The full list of analyzers and report folders is in Appendix A of the paper, section 19.
