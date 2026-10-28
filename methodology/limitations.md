# Limitations

> Extracted from the paper ([paper/paper.md](../paper/paper.md)). Section numbers refer to the paper.

# 16. Limitations

## 16.1 Data

- **One user.** The corpus comes from one heavy user on one machine, with several clients and
  model generations. Shares and ratios describe this workload and are not population estimates.
- **Period.** The corpus predates Codex's October 2026 storage changes. Post-change behaviour
  is measured on three pairs, four recent sessions and the sizes of 843 `.zst` files.
- **Files and sessions.** Files are not sessions. Forks, resumes and subagents produce related
  files, and physical record counts include replayed records.
- **Modification times.** The 843 Codex `.zst` files were matched by name only, without
  comparing modification times. Sessions continued after our copy would make Codex's sizes
  larger, and so overstate our advantage in Section 14.

## 16.2 Measurement

- **Exact matching only.** Duplication is exact byte or hash identity. Semantically equivalent,
  re-encoded or resized content counts as distinct, so duplication is a lower bound.
- **Bytes, not tokens.** No storage share is converted into tokens or cost.
- **Token evidence.** It rests on recorded counters and diagnostic cases:
  - one fully reconstructed turn;
  - 21 paired requests from local trace logs;
  - 74 snapshot-attached usage readings.

  No server-side tokenizer, per-item attribution or billing data were available.
- **Durations.** They rely on client timestamps, present for 755 of 1,314 compaction events.
- **Privacy by design.** Tool names, arguments and content were not exported. Results are
  therefore broken down by record subtype, not by tool or by content kind; pasted text, for
  example, is not separated from typed text.

## 16.3 Inference

- **Compaction path.** The path used in practice, local or remote, was not observed. It is
  inferred from code and from images retained in recent snapshots.
- **Resume.** Its requirements are derived from code and structure, not from controlled resume
  experiments.
- **Legacy rewrite.** Its rules are inferred from three before/after pairs; the code responsible
  was not located.
- **Subagents.** Corpus-wide subagent duplication is hypothesized from one measured case and
  size signatures.

## 16.4 Not measured

| Item | Where it matters |
|---|---|
| Resume memory and latency, for any format | Sections 9.5, 14 |
| Equivalence of context reconstructed from a rewritten rollout | Section 12 |
| Token effect of sending images by reference or loading them on demand | Section 13 |
| Time and token effect of incremental compaction and of a project memory file | Section 13 |
| Presence in the parent file of the 10 images missing from the rewritten subagent file | Section 11.4 |

These are the measurements a follow-up study, or OpenAI with server-side data, should perform
first.
