# Analysis code

These are the analyzers and synthetic tests that produced the measurements in the paper. The
code is published for inspection and reuse. The private session files it was run against are not
published, so the measurements cannot be re-run from this repository alone.

License: MIT (see [../LICENSE-CODE](../LICENSE-CODE)). The paper and data are under CC BY 4.0.

## What was changed for publication

- **Placeholders for case studies.** A few analyzers hard-code the case-study files they examine
  (`analyze_output_token_components.py`, `analyze_session_storage_tokens.py`,
  `analyze_turn_token_timeline.py`, `compare_media_reference.py`). Their real file names, session
  identifiers and timestamps were replaced with placeholders: `2026/01/01/rollout-2026-01-01T00-00-NN-`
  plus a placeholder ID `00000000-0000-4000-8000-…`. To run them, set these constants to your own
  files.
- **Synthetic test data.** Test fixtures that had been derived from real identifiers now use
  synthetic identifiers and times, chosen so that each test checks the same behaviour as before.
- **Database tests.** Tests that need a live PostgreSQL database are skipped when no `.env`
  configuration file is present.

Analysis logic is unchanged.

## Requirements

- **Python.** Standard library only. Python 3.14 or later is needed for the zstd comparisons
  (`compression.zstd`); the other analyzers also run on Python 3.12.
- **Database.** PostgreSQL 18 for the record index and token observations. The schema is in
  [`../database/migrations/`](../database/migrations/) (`001`–`006`). `psql` is used through
  `subprocess`.
- **Connection settings.** A `.env` file at the repository root, never committed, with
  `DB_HOST`, `DB_PORT`, `DB_DATABASE`, `DB_USERNAME` and `DB_PASSWORD`.
- **Data layout.** The analyzers expect rollout files under `2026/` at the repository root, with
  outputs written to `data/reports/<analysis>_v<N>/`. Neither folder is published.

## Running the tests

```bash
python -B -m unittest discover -s scripts -p "test_*.py" -v
```

The suite runs 225 tests on synthetic fixtures and reads no research data. Without `.env`, the 7
database tests are skipped, and all other tests pass. Database contract tests use temporary tables
inside a rolled-back transaction.

## Rules every analyzer follows

- **Read-only.** Sources are opened read-only, with size and modification time verified before
  and after each file.
- **Streaming.** Files are read in 256 KiB chunks, by a single process at below-normal priority,
  with atomic per-file checkpoints.
- **Aggregates only.** Outputs hold counts, byte sizes, hashes, field paths and short
  enumerations, never content.
- **Plan first.** Default modes are read-only plans or dry runs; database writes require `--apply`.
- **Loud failures.** Parse errors and limits stop a run with an error.

## Analyzers by stage

| Stage | Scripts | Paper section |
|---|---|---|
| Inventory and survey | `survey_sessions.py`, `fast_session_index.py`, `import_session_survey.py`, `import_session_index.py` | §4 |
| Record index | `index_session_records.py`, `repair_null_record_types.py`, `export_session_statistics.py`, `resolve_compound_session_ids.py`, `apply_compound_session_ids.py` | §6, §7.2 |
| Structural censuses | `profile_record_shapes.py`, `profile_response_items.py`, `profile_compacted.py` | §7–§9 |
| Hash indexes | `profile_compacted_images.py`, `profile_compacted_texts.py` | §11 |
| Growth and composition | `analyze_compaction_growth.py`, `analyze_compaction_text_growth.py`, `analyze_storage_composition.py`, `analyze_storage_composition_event_projection.py` | §7.3, §9.2 |
| Token observations | `index_token_usage.py`, `token_usage_stream.py`, `mark_token_usage_duplicates.py` | §10 |
| Token case studies | `analyze_session_storage_tokens.py`, `analyze_output_token_components.py`, `analyze_turn_token_timeline.py`, `audit_model_request_capture.py`, `analyze_logs_request_capture.py`, `analyze_logs_request_comparison.py`, `analyze_tokens_vs_storage.py`, `analyze_length_control.py`, `analyze_compaction_cost.py` | §9.3, §10 |
| Local state | `export_sqlite_tables.py`, `import_codex_sqlite.py` | §4.2 |
| Before/after | `compare_zstd_pairs.py`, `compare_zstd_stage2.py`, `list_zst_counterparts.py` | §12 |
| Reference variants | `compare_media_reference.py`, `compare_media_reference_v2.py` | §14 |
| Figures | `make_figures.py`: redraws the six figures, as SVG next to the script, from the aggregate values | figures/ |

Helpers and sampling tools: `extract_record_sample.py`, `extract_record_type_samples.py`,
`format_sample_json.py`, `create_readable_sample_views.py`, and the SQL queries
`tokens_vs_storage_query.sql`, `length_control_query.sql`, `compaction_growth_query.sql`,
`compaction_cost_snapshot.sql`.
