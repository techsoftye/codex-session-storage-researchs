# Codex Session Storage Research

**Session Storage Amplification in Long-Horizon Coding Agents: Measurement, Compaction, and a Reference-Based Persistence Architecture**

Eng Rassam · October 2026

An empirical study of 52.49 GiB of real Codex session rollouts: 845 files written between
January and October 2026. It measures what the agent stores, why the stored files grow, how
recorded token use relates to stored size, and how much smaller a reference-based design is.

- 📄 Paper: [paper/paper.pdf](paper/paper.pdf) · [paper/paper.md](paper/paper.md)
- 📊 Aggregated results: [results/](results/)
- 🧪 Method and limitations: [methodology/](methodology/)

## Key findings

| Finding | Value |
|---|---|
| Share of stored bytes that are compaction snapshots | **77.13%** |
| Share that are images already present in the previous snapshot | **69.34%** |
| Share that is the user-role and assistant conversation (without images: 0.64%) | **2.89%** |
| Stored bytes ÷ conversation bytes (corpus-weighted; median per file 14.7×) | **34.6×** |
| Consecutive snapshots that are identical (sample) | **~98%** |
| Median compaction duration (n = 755; unrelated to snapshot size) | **56.5 s** |
| File size vs. tokens used, controlling for session length (partial Spearman, n = 189) | **−0.019** |
| Codex's October 2026 rewrite and compression on 843 files | 52.48 → **10.60 GiB** |
| Images by reference with a media table shared across sessions, same zstd level | **3.00 GiB** (3.53× smaller than Codex's current format) |

![Where the stored bytes go](figures/storage-composition.png)

![Stored size of the same 843 sessions](figures/storage-comparison.png)

All other figures: [figures/](figures/).

## Repository contents

```text
paper/          the paper (PDF and Markdown)
figures/        six figures as PNG
results/        aggregated-metrics.json (all headline numbers, each with its source report)
                comparison-summary.csv (Codex format against the reference-based variants)
methodology/    measurement methodology and limitations, extracted from the paper
scripts/        description of the analysis pipeline
```

## Data and privacy

The study analysed the author's own Codex sessions on the author's machine:

- Every measurement was produced by streaming scripts that read copies of the files read-only.
- The scripts kept only byte counts, hashes, structural field paths and counters.
- No conversation text, images, commands, paths or session identifiers appear in this
  repository.

The raw session files and the intermediate per-file reports are **not** published. Numbers in the
paper cite the report they come from, for example `data/reports/storage_composition_v1`. These
paths refer to the author's private working directory. The published aggregates are in
[results/](results/).

## Scope

- **One user.** This is a measurement study of one heavy user's corpus. Shares describe this
  workload, not all Codex users.
- **Bytes, not tokens.** Storage bytes are not tokens, and no cost is estimated.
- **Labelled statements.** Statements in the paper are labelled Observed, Inferred,
  Hypothesized or Proposed.

See [methodology/limitations.md](methodology/limitations.md).

## Citation

See [CITATION.cff](CITATION.cff).

## License

The paper, figures and data in this repository are released under the Creative Commons
Attribution 4.0 International license (CC BY 4.0). See [LICENSE](LICENSE).
