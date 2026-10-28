"""Generate the paper's three static SVG figures from report figures (byte counts only).

Every value below is copied from the cited report and is also tabulated in the paper text.
Static figures for print: the light palette only, with no hover layer. The table in each
section is the accessible table view.
"""
from pathlib import Path
from xml.sax.saxutils import escape

OUT = Path(__file__).resolve().parent
GIB = 2 ** 30

SURFACE, INK, INK2, MUTED, GRID, AXIS = '#fcfcfb', '#0b0b0b', '#52514e', '#898781', '#e1e0d9', '#c3c2b7'
BLUE, ORANGE = '#2a78d6', '#eb6834'    # categorical slots 1-2, validated (validate_palette.js, light)
FONT = "system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif"


def bar_chart(name, title, subtitle, rows, axis_max, step, unit, fmt, source, legend=None, axis_min=0):
    """rows: (label, value, color). Horizontal bars from one baseline, values at the tip."""
    width, label_w, plot_w, bar_h, row_h = 740, 330, 300, 18, 34
    sub_lines = subtitle.split('\n')
    extra = 18 * (len(sub_lines) - 1)
    top = (92 if legend else 76) + extra
    height = top + row_h * len(rows) + 64
    x0 = label_w + 16
    scale = plot_w / (axis_max - axis_min)
    zx = x0 - axis_min * scale
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
           f'viewBox="0 0 {width} {height}" font-family="{FONT}" role="img" aria-labelledby="t d">',
           f'<title id="t">{escape(title)}</title><desc id="d">{escape(subtitle)}</desc>',
           f'<rect width="{width}" height="{height}" fill="{SURFACE}"/>',
           f'<text x="24" y="32" font-size="17" font-weight="600" fill="{INK}">{escape(title)}</text>',
           *[f'<text x="24" y="{54 + 18 * k}" font-size="13" fill="{INK2}">{escape(line)}</text>'
             for k, line in enumerate(sub_lines)]]
    if legend:
        lx = 24
        for text, color in legend:
            out.append(f'<rect x="{lx}" y="{66 + extra}" width="12" height="12" rx="2" fill="{color}"/>')
            out.append(f'<text x="{lx + 18}" y="{76.5 + extra}" font-size="12.5" fill="{INK2}">{escape(text)}</text>')
            lx += 26 + 7.2 * len(text)
    plot_top, plot_bottom = top, top + row_h * len(rows)
    tick = axis_min
    while tick <= axis_max + 1e-9:
        tick = round(tick, 6)
        x = zx + tick * scale
        out.append(f'<line x1="{x:.1f}" y1="{plot_top}" x2="{x:.1f}" y2="{plot_bottom}" '
                   f'stroke="{AXIS if tick == 0 else GRID}" stroke-width="1"/>')
        out.append(f'<text x="{x:.1f}" y="{plot_bottom + 18}" font-size="12" fill="{MUTED}" '
                   f'text-anchor="middle">{fmt_tick(tick)}{unit}</text>')
        tick += step
    for i, (label, value, color) in enumerate(rows):
        cy = plot_top + row_h * i + row_h / 2
        y = cy - bar_h / 2
        w = max(abs(value) * scale, 1.5)
        r = min(4, w)
        out.append(f'<text x="{label_w}" y="{cy + 4.5:.1f}" font-size="13" fill="{INK}" '
                   f'text-anchor="end">{escape(label)}</text>')
        if value >= 0:
            out.append(f'<path d="M{zx:.1f},{y:.1f} h{w - r:.1f} a{r},{r} 0 0 1 {r},{r} v{bar_h - 2 * r} '
                       f'a{r},{r} 0 0 1 -{r},{r} h-{w - r:.1f} z" fill="{color}"/>')
            tip = zx + w + 6
        else:
            out.append(f'<path d="M{zx:.1f},{y:.1f} h-{w - r:.1f} a{r},{r} 0 0 0 -{r},{r} v{bar_h - 2 * r} '
                       f'a{r},{r} 0 0 0 {r},{r} h{w - r:.1f} z" fill="{color}"/>')
            tip = zx + 6    # value label right of the baseline; the minus sign carries the direction
        out.append(f'<text x="{tip:.1f}" y="{cy + 4.5:.1f}" font-size="13" fill="{INK2}" '
                   f'font-variant-numeric="tabular-nums">{escape(fmt(value))}</text>')
    out.append(f'<text x="24" y="{height - 16}" font-size="11.5" fill="{MUTED}">{escape(source)}</text>')
    out.append('</svg>')
    (OUT / name).write_text('\n'.join(out) + '\n', encoding='utf-8')


def fmt_tick(v):
    return f'{v:g}'.replace('-', '−')


def figure_1():
    total = 56_355_653_690    # record_type_counts_corrected_v1.json
    parts = [                 # storage_composition_v1 (disjoint categories)
        ('Compacted: images repeated from previous snapshot', 39_077_030_034),
        ('Processing: reasoning, tool calls and outputs', 5_339_656_937),
        ('Processing: execution and runtime events', 3_136_268_147),
        ('Compacted: all other bytes', 3_052_068_324),
        ('Conversation: user-role and assistant messages', 1_627_855_999),
        ('Compacted: text repeated from previous snapshot', 1_335_546_618),
        ('State and metadata records', 1_102_057_313),
        ('Logging copies of messages', 995_650_129),
        ('Token counters', 626_577_310),
        ('Other response items', 62_942_879),
    ]
    assert sum(v for _, v in parts) == total
    rows = [(label, v / total * 100, ORANGE if label.startswith('Conversation') else BLUE) for label, v in parts]
    bar_chart('fig1_storage_composition.svg',
              'Figure 1. Where the stored bytes go',
              'Share of all 52.49 GiB in 845 Codex rollout files, disjoint categories (sum = 100%)',
              rows, 70, 10, '%', lambda v: f'{v:.2f}%',
              'Source: data/reports/storage_composition_v1, record_type_counts_corrected_v1.json',
              legend=[('Storage category', BLUE), ('The conversation itself', ORANGE)])


def figure_2():
    total = 43_464_644_976    # compacted_item_sizes_v1
    parts = [
        ('replacement_history: embedded images', 39_210_063_415),
        ('guardian_history (all items)', 2_387_178_293),
        ('replacement_history: message text', 1_667_585_210),
        ('Compaction summary (encrypted)', 153_735_880),
        ('Agent messages', 40_621_567),
        ('Delimiters, identifiers, metadata', 5_460_611),
    ]
    assert sum(v for _, v in parts) == total
    rows = [(label, v / total * 100, BLUE) for label, v in parts]
    bar_chart('fig2_compacted_components.svg',
              'Figure 2. What a compacted snapshot contains',
              'Share of all compacted bytes (8,363 snapshots, 40.48 GiB)',
              rows, 100, 20, '%', lambda v: f'{v:.2f}%' if v >= 0.1 else f'{v:.3f}%',
              'Source: data/reports/compacted_item_sizes_v1')


def figure_3():
    parts = [                 # media_reference_comparison_v2/summary_corpus.json (843 files)
        ('Original files (pre-October)', 56_353_330_276, BLUE),
        ('Codex current format (.zst)', 11_381_948_312, BLUE),
        ('Images by reference, media per session', 3_853_059_232, ORANGE),
        ('Images by reference, shared media table', 3_225_189_819, ORANGE),
    ]
    rows = [(label, v / GIB, color) for label, v, color in parts]
    bar_chart('fig3_theirs_vs_ours.svg',
              'Figure 6. Stored size of the same 843 sessions',
              'GiB on disk, zstd level 3 throughout; shared media is 3.53× smaller than the Codex .zst files.\n'
              'Codex side: post-October rewrite. Variants: pre-October originals, matched by file name.',
              rows, 60, 10, '', lambda v: f'{v:.2f} GiB',
              'Source: data/reports/media_reference_comparison_v2, zst_counterpart_sizes_v1',
              legend=[('Codex', BLUE), ('Reference-based variants (this paper)', ORANGE)])


def figure_4():
    parts = [                 # compacted_image_duplicates_v1, disjoint categories (stored JSON bytes)
        ('Same image, later snapshot of the same file', 38_045_175_558),
        ('Extra copy within the same snapshot', 1_068_305_807),
        ('First copy of each distinct image', 879_067_554),
        ('Same image in another file', 752_086_625),
        ('Same image in both histories of a snapshot', 93_794_408),
    ]
    assert sum(v for _, v in parts) == 40_838_429_952
    rows = [(label, v / GIB, ORANGE if label.startswith('First') else BLUE) for label, v in parts]
    bar_chart('fig4_image_duplication.svg',
              'Figure 5. Embedded images in compacted snapshots',
              '99,244 appearances of 1,936 distinct images (0.61 GiB decoded); stored JSON string bytes, GiB',
              rows, 40, 10, '', lambda v: f'{v:.2f} GiB',
              'Source: data/reports/compacted_image_duplicates_v1',
              legend=[('Repeated copies', BLUE), ('Retained first copy', ORANGE)])


def figure_5():
    parts = [                 # compaction_growth_v1 (845 files)
        ('No compacted record', 411), ('1 snapshot', 158), ('2–5 snapshots', 97),
        ('6–10 snapshots', 34), ('11–25 snapshots', 63), ('26–50 snapshots', 30),
        ('51–100 snapshots', 18), ('101 or more snapshots', 34),
    ]
    assert sum(v for _, v in parts) == 845
    rows = [(label, v, BLUE) for label, v in parts]
    bar_chart('fig5_compaction_growth.svg',
              'Figure 3. Compaction snapshots per session file',
              'Number of files (845); 34 files hold 101 or more full snapshots each',
              rows, 450, 50, '', lambda v: f'{v:,.0f}',
              'Source: data/reports/compaction_growth_v1')


def figure_6():
    parts = [                 # tokens_vs_storage_v1/length_control.md (189 files)
        ('Record count', 0.959559, BLUE),
        ('File size', 0.931433, BLUE),
        ('File size, controlling for record count', -0.018564, ORANGE),
        ('Compacted bytes', 0.869330, BLUE),
        ('Compacted bytes, controlling for record count', 0.133363, ORANGE),
    ]
    rows = [(label, v, color) for label, v, color in parts]
    bar_chart('fig6_token_correlation.svg',
              'Figure 4. What tracks recorded token use',
              'Spearman correlation with tokens_used, 189 session files ending before 19 June 2026',
              rows, 1.0, 0.2, '', lambda v: f'{v:+.3f}'.replace('-', '−'),
              'Source: data/reports/tokens_vs_storage_v1 (length_control.md, report.md)',
              legend=[('Unadjusted', BLUE), ('Partial, controlling for record count', ORANGE)],
              axis_min=-0.2)


if __name__ == '__main__':
    figure_1()
    figure_2()
    figure_3()
    figure_4()
    figure_5()
    figure_6()
    print('wrote', ', '.join(sorted(p.name for p in OUT.glob('fig*.svg'))))
