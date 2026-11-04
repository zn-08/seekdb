#!/usr/bin/env python3
"""Compare two fts_large_bench.sh report sets measured on the same machine.

The challenge scores against a fixed baseline captured on the VLDB CI machine, so a
different runner cannot reproduce the official number.  What a local comparison can
show reliably is the *relative* effect of a patch, which is what decides whether an
optimisation is worth keeping.  This script therefore compares a "before" report set
with an "after" report set that were produced on one machine, with one configuration.

Usage:
  fts_ab_compare.py --before before_1.txt [before_2.txt ...] \
                    --after after_1.txt [after_2.txt ...] \
                    [--baseline fts_large_bench_baseline.json] [--json result.json]
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
from pathlib import Path
from typing import Any

METRIC_RE = re.compile(r"^\s*([A-Za-z0-9_]+):\s*([-+]?(?:\d+(?:\.\d*)?|\.\d+))\s*$")
DEFAULT_BASELINE = Path(__file__).with_name("fts_large_bench_baseline.json")

# Metrics that only exist for reference; they are reported but never scored.
AUXILIARY = {
    "select1_avg_ms",
    "select1_stdev_ms",
    "raw_load_sec",
    "raw_load_rows_per_sec",
    "build_total_sec",
}

# Result counters.  They are not scored, but they must not move: a kernel that
# returns different rows is not an optimisation, it is a regression.
HIT_METRICS = {
    "query_cn_hits",
    "query_beng_hits",
    "query_mixed_hits",
    "query_limit_hits",
}


def load_report(path: Path) -> dict[str, float]:
    metrics: dict[str, float] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        match = METRIC_RE.match(line)
        if match:
            metrics[match.group(1)] = float(match.group(2))
    return metrics


def aggregate(paths: list[Path]) -> dict[str, float]:
    reports = [load_report(path) for path in paths]
    keys = set(reports[0])
    for report in reports[1:]:
        keys &= set(report)
    # The median over repetitions is far less noise sensitive than a single run.
    return {key: statistics.median(report[key] for report in reports) for key in sorted(keys)}


def official_score(report: dict[str, float], baseline: dict[str, Any]) -> dict[str, Any]:
    """Reproduce fts_large_bench_score.py for a report measured on this machine.

    The number is informational only: the official score is computed on the fixed
    evaluation machine, so it differs from anything measured elsewhere.
    """
    baseline_metrics = baseline["baseline_metrics"]
    categories = baseline["score_categories"]
    full_score = float(baseline["score_rule"]["full_score_improvement"])
    max_score = float(baseline["score_rule"]["max_score"])

    category_improvement = {}
    for category, metrics in categories.items():
        improvements = []
        for metric in metrics:
            base = float(baseline_metrics[metric])
            current = float(report[metric])
            improvements.append((base - current) / base)
        category_improvement[category] = statistics.fmean(improvements)

    mean_improvement = statistics.fmean(category_improvement.values())
    normalized = max(0.0, min(mean_improvement, full_score))
    return {
        "score": normalized / full_score * max_score,
        "mean_improvement": mean_improvement,
        "categories": category_improvement,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", nargs="+", required=True, type=Path)
    parser.add_argument("--after", nargs="+", required=True, type=Path)
    parser.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--json", type=Path)
    args = parser.parse_args()

    baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
    before = aggregate(args.before)
    after = aggregate(args.after)

    categories = baseline["score_categories"]
    scored_metrics = [metric for metrics in categories.values() for metric in metrics]

    result: dict[str, Any] = {
        "repetitions": {"before": len(args.before), "after": len(args.after)},
        "metrics": {},
        "categories": {},
    }

    for metric in scored_metrics:
        if metric not in before or metric not in after:
            continue
        base_value = before[metric]
        new_value = after[metric]
        improvement = (base_value - new_value) / base_value
        result["metrics"][metric] = {
            "before": base_value,
            "after": new_value,
            "improvement": improvement,
        }

    for category, metrics in categories.items():
        improvements = [
            result["metrics"][metric]["improvement"]
            for metric in metrics
            if metric in result["metrics"]
        ]
        if improvements:
            result["categories"][category] = statistics.fmean(improvements)

    if result["categories"]:
        result["mean_improvement"] = statistics.fmean(result["categories"].values())
    else:
        result["mean_improvement"] = 0.0

    for name, report in (("before", before), ("after", after)):
        if all(metric in report for metric in scored_metrics):
            result[f"official_style_{name}"] = official_score(report, baseline)

    print("FTS A/B comparison (same machine, same configuration)")
    print("=" * 72)
    print(f"repetitions: before={len(args.before)} after={len(args.after)}")
    print()
    print(f"{'metric':<26}{'before':>14}{'after':>14}{'improvement':>16}")
    print("-" * 72)
    for metric in scored_metrics:
        entry = result["metrics"].get(metric)
        if not entry:
            continue
        print(
            f"{metric:<26}{entry['before']:>14.5f}{entry['after']:>14.5f}"
            f"{entry['improvement'] * 100:>15.2f}%"
        )
    print("-" * 72)
    for category, improvement in result["categories"].items():
        print(f"{category:<26}{'':<14}{'':<14}{improvement * 100:>15.2f}%")
    print("-" * 72)
    print(f"{'mean':<26}{'':<14}{'':<14}{result['mean_improvement'] * 100:>15.2f}%")
    print()
    for metric in sorted(AUXILIARY):
        if metric in before and metric in after:
            print(f"  (aux) {metric:<24}{before[metric]:>14.5f}{after[metric]:>14.5f}")

    # A faster kernel is only worth anything if it returns the same answers.
    correctness = []
    for metric in sorted(HIT_METRICS):
        if metric in before and metric in after:
            correctness.append({"metric": metric, "before": before[metric], "after": after[metric],
                                "identical": before[metric] == after[metric]})
    result["correctness"] = {
        "hit_metrics": correctness,
        "passed": bool(correctness) and all(item["identical"] for item in correctness),
    }
    print()
    print("correctness (query hit counts must not change):")
    for item in correctness:
        flag = "same" if item["identical"] else "CHANGED"
        print(f"  {item['metric']:<24}{item['before']:>14.5f}{item['after']:>14.5f}   {flag}")
    if not result["correctness"]["passed"]:
        print("\nWARNING: the candidate patch changed the query results.")

    print()
    for name in ("before", "after"):
        entry = result.get(f"official_style_{name}")
        if entry:
            print(
                f"official-style score against the fixed baseline ({name}): "
                f"{entry['score']:.2f}/100  mean_improvement={entry['mean_improvement'] * 100:.2f}%"
            )
    if result["mean_improvement"] <= 0:
        print("\nWARNING: the candidate patch does not improve the measured workload.")

    if args.json:
        args.json.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
