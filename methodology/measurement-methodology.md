# Measurement methodology

> Extracted from the paper ([paper/paper.md](../paper/paper.md)). Section numbers refer to the paper.

# 5. Measurement Taxonomy

## 5.1 Units of observation

| Term | Meaning |
|---|---|
| **File** | One rollout JSONL file. Not necessarily one logical session. |
| **Physical record** | One line of a file. Records replayed on resume or copied from a parent are counted each time they are stored, so record counts are not message, request or action counts. |
| **Record type** | The top-level `type` field: `compacted`, `response_item`, `event_msg`, `world_state`, `session_meta`, `turn_context`, `token_usage_record`, `inter_agent_communication_metadata`. |
| **Subtype** | `payload.type` within a record type, e.g. `reasoning` or `function_call_output` in `response_item`, or `token_count` in `event_msg`. |
| **Snapshot** | One `compacted` record, with its `replacement_history` (the retained context) and optional `guardian_history` (history kept for the safety reviewer). |
| **Item** | One element of a snapshot's history arrays. |

## 5.2 Functional categories

Every stored byte is assigned to exactly one category, so the categories sum to the corpus size
(Section 7.3; *storage_composition_v1*):

| Category | Members |
|---|---|
| **Conversation** | `response_item` messages with role `user` or `assistant`. The `user` role includes context injected by the client and does not establish human authorship. |
| **Processing** | `response_item` reasoning, tool calls and tool outputs; `event_msg` execution and runtime events. |
| **Logging copies** | `event_msg` records that repeat conversation messages (`user_message`, `agent_message`, and message items of `item_completed`). |
| **Token counters** | `event_msg/token_count`. |
| **State and metadata** | `world_state`, `session_meta`, `turn_context`, `token_usage_record`, inter-agent metadata. |
| **Compacted** | All `compacted` bytes, further split into: images present in the preceding snapshot, text blocks present in the preceding snapshot, and all other bytes. |
| **Other** | Remaining `response_item` subtypes (developer messages, `compaction`, `agent_message`, `ghost_snapshot`). |

## 5.3 Duplication

- **Exact identity only.**
  - Images are identical when the SHA-256 of their decoded bytes matches.
  - A text block is one selected JSON string, compared by SHA-256 of its unescaped UTF-8.
  - An item is compared by its exact serialized bytes.

  Re-encoded, resized or reworded content counts as distinct.
- **Retained copy.** For duplicate accounting, the first occurrence in file, line and JSON
  traversal order is retained; all later occurrences are duplicates.
- **Carry-forward.** An occurrence also present in the immediately preceding snapshot of the
  same file. This is a subset of within-file duplication.
- **Stored versus decoded bytes.** Stored bytes include JSON quoting, escaping and data-URL
  prefixes; decoded bytes are the binary image size. The two are never subtracted from each other.

## 5.4 Tokens

- **Counters.** Token counters are read only where Codex recorded them: input, cached input,
  cache-write input, output, reasoning output and total.
  - total = input + output; cached ⊂ input; reasoning ⊂ output.
  - A missing counter is unknown, not zero.
- **Scopes.** Response, turn, thread, last and cumulative scopes are alternative views of the
  same consumption and are never added together.
- **Duplicate flags.** A row flagged as a possible duplicate is a candidate, not evidence of a
  duplicate charge.
- **Bytes are not tokens.** No storage share is converted into a token share, and no cost is
  computed.

## 5.5 Statement labels

| Label | Meaning |
|---|---|
| **[O] Observed** | Measured directly from the data or read from public code. |
| **[I] Inferred** | Follows from observations by stated reasoning. |
| **[H] Hypothesized** | Consistent with observations but not verified. |
| **[P] Proposed** | A design or recommendation. |

Each statement also identifies its period: the pre-October corpus, the October 2026 code, or the
post-October samples.

## 5.6 Units

| Unit | Bytes |
|---|---:|
| GB | 10⁹ |
| GiB | 2³⁰ |
| MB | 10⁶ |
| MiB | 2²⁰ |

Corpus-weighted ratios divide summed numerators by summed denominators. Per-file statistics
(median, P10, P90) describe the typical file. Unweighted per-file means are not used for
conclusions.

---

# 6. Methodology

## 6.1 Pipeline

The analysis ran as a sequence of separately approved stages. Each stage writes a versioned
report folder (`data/reports/<analysis>_v<N>/`) and a dated entry in the project log. A new
version never overwrites an earlier one.

| Stage | Method | Output |
|---|---|---|
| 1. Inventory | File-system metadata only: names, sizes, timestamps | `DATASET_INVENTORY.md` |
| 2. Record index | One sequential pass in 1 MiB chunks. Stores one locator per physical line (file, line number, byte offset, byte length, timestamp text, top-level type), keeping at most 64 KiB of a line's start | PostgreSQL `research_session_records` (3,497,535 rows) |
| 3. Type repair | 535 untyped records traced to a UTF-8 decode cut inside a multibyte character; repaired from 35 MB of record headers | `null_record_type_repair_v1.json`, `STRUCTURAL_ANALYSIS_STEPS_1_3.md` |
| 4. Structural censuses | Streaming parsers over indexed byte ranges of `response_item`, `event_msg` and `compacted`. They export only field paths, JSON types, short allow-listed enumerations, counts and byte sizes | `*_profile_v1`, `compacted_item_sizes_v1` |
| 5. Hash indexes | Images decoded incrementally and hashed (SHA-256); text blocks hashed after JSON unescaping. Indexes hold hashes, lengths and locators in SQLite | `compacted_image_duplicates_v1`, `compacted_text_duplicates_v1` |
| 6. Growth and composition | Queries over locators and hash indexes: snapshot growth, carry-forward, functional decomposition | `compaction_growth_v1`, `storage_composition_v1` |
| 7. Token observations | Every JSON object carrying a token counter, in every record type, with exact locator and identifiers. Duplicate candidates flagged by rule, never deleted | `research_token_usage_observations` (1,778,473 rows); `token_usage_duplicate_candidates_v1` |
| 8. Token case studies | Turn timeline, output components, request bodies from the local trace log, storage–token association with length control | `turn_token_timeline_v1`, `output_token_components_v1`, `logs_request_capture_v1`, `tokens_vs_storage_v1` |
| 9. Local state | SQLite stores exported and imported, with text values reduced to lengths and hashes | `SQLITE_SCHEMA.md`, `sqlite_metadata_import_v1` |
| 10. Before/after | Codex `.zst` copies decompressed and compared record by record with pre-change copies; Codex `.zst` sizes listed by name | `zstd_comparison/`, `zst_counterpart_sizes_v1` |
| 11. Reference variants | Each file streamed once; variants compressed in memory; byte counts only | `media_reference_comparison_v1`, `media_reference_comparison_v2` |

## 6.2 Resource discipline

Every script follows the same rules:
- **Streaming.** Files are read in 256 KiB chunks (1 MiB for the record index). No whole file,
  history or long string is loaded into memory. Where an item had to be held in memory, its
  maximum size is reported (238 KB in Section 14).
- **Scheduling.** One process at below-normal priority, with no parallel workers.
- **Restart.** Atomic per-file checkpoints allow a run to resume.
- **Integrity.** Source size and modification time are verified before and after each file.
- **Failure handling.** Parse errors, unknown structures and limit hits stop the run with an
  error. Nothing is skipped silently and reported as complete.
- **Measurement.** Runtime and peak working set are recorded in each report. For example, the
  corpus-wide reference evaluation read 52.48 GiB in 274 s with a peak working set of 42 MiB.

## 6.3 Verification

- **Synthetic tests.** Every analyzer has unit tests on synthetic fixtures. They cover chunk
  boundaries, escapes, truncated lines and byte-exact reconstruction for the reference variants.
  No test reads research data.
- **Reconciliation.**
  - Per-type bytes sum to the corpus size.
  - Functional categories sum to the corpus size.
  - Compacted partitions sum to the compacted total.
  - Independent analyzers agree, for example on 8,363 snapshots, 1,424,366 history items and
    99,244 images in history items.
- **Calibration.** Python's zstd level 3 reproduced the size of Codex's own `.zst` files byte for
  byte, so the compressed comparisons use the same compressor settings.

## 6.4 Software

- Python standard library only; no third-party packages. Python 3.14 for `compression.zstd`.
- PostgreSQL 18 for locators and observations: migrations `001`–`006`.
- SQLite for hash indexes and copied Codex stores.

Commands, limits and caveats for every script are listed in `scripts/README.md` (Appendix).

## 6.5 Limits of the method

- One user's corpus.
- Exact-match duplication only.
- Client timestamps for durations.
- No server-side token data.
- No direct observation of which compaction path ran.
- Resume memory and latency were not measured.
