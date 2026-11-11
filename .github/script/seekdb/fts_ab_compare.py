#!/usr/bin/env python3
"""Compare two fts_large_bench.sh reports that were produced on one machine.

The official scoring script compares a report against the fixed baseline that
was measured on the grading machine, which says nothing about a change: the
machine that produced the report may be much faster or much slower than the
machine that produced the baseline.  The only meaningful comparison of two
commits is the one of two reports that were produced by the same runner, and
this script applies the rule of the official script with the report of the
base commit as the baseline.

usage: fts_ab_compare.py <base report> <candidate report> [...]
"""

from __future__ import annotations

import sys

CATEGORIES = {
    "build": ["build_ik_all_sec", "build_ik_content_sec", "build_beng_en_sec"],
    "tokenize": ["tokenize_ik_avg_ms", "tokenize_beng_avg_ms"],
    "query": [
        "query_cn_avg_ms",
        "query_beng_avg_ms",
        "query_mixed_avg_ms",
        "query_limit_avg_ms",
    ],
}

HITS = [
    "query_cn_hits",
    "query_beng_hits",
    "query_mixed_hits",
    "query_limit_hits",
]


def read(path: str) -> dict:
    values = {}
    with open(path, encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if ":" not in line:
                continue
            key, _, value = line.partition(":")
            try:
                values[key.strip()] = float(value.strip())
            except ValueError:
                values[key.strip()] = value.strip()
    return values


def compare(base: dict, candidate: dict) -> str:
    lines = []
    improvements = {}
    for category, metrics in CATEGORIES.items():
        category_improvements = []
        for metric in metrics:
            before = base[metric]
            after = candidate[metric]
            improvement = (before - after) / before
            category_improvements.append(improvement)
            lines.append(
                f"  {metric:<24} {before:>12.5f} {after:>12.5f} {improvement * 100:>8.2f}%"
            )
        improvements[category] = sum(category_improvements) / len(category_improvements)
        lines.append(f"  -> {category}: {improvements[category] * 100:.2f}%")
    mean = sum(improvements.values()) / len(improvements)
    lines.append(
        f"  => mean improvement {mean * 100:.2f}%  -> official score "
        f"{max(0.0, min(mean, 0.5)) / 0.5 * 100:.2f}/100"
    )
    for hit in HITS:
        if base.get(hit) != candidate.get(hit):
            lines.append(f"  !! hit count changed: {hit} {base.get(hit)} -> {candidate.get(hit)}")
    return "\n".join(lines)


def main() -> int:
    base = read(sys.argv[1])
    for path in sys.argv[2:]:
        print(f"===== {path}")
        print(compare(base, read(path)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
