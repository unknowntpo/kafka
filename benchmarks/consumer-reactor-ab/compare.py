#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Compare independent baseline and reactor benchmark samples without dependencies."""

import argparse
import csv
import itertools
import math
import statistics
import sys
from pathlib import Path


PASS = 0
FAIL = 1
INCONCLUSIVE = 2
USAGE_ERROR = 3


def median_absolute_deviation(values):
    center = statistics.median(values)
    return statistics.median(abs(value - center) for value in values)


def doubled_ranks(values):
    """Return average ranks multiplied by two so tied ranks remain exact integers."""
    ordered = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0] * len(values)
    start = 0
    while start < len(ordered):
        end = start + 1
        while end < len(ordered) and ordered[end][1] == ordered[start][1]:
            end += 1
        # Ranks are one-based. Twice the average of [start + 1, end].
        rank_times_two = start + 1 + end
        for offset in range(start, end):
            ranks[ordered[offset][0]] = rank_times_two
        start = end
    return ranks


def u_statistic_times_two(sample_a, sample_b):
    ranks = doubled_ranks(sample_a + sample_b)
    rank_sum_times_two = sum(ranks[:len(sample_a)])
    return rank_sum_times_two - len(sample_a) * (len(sample_a) + 1)


def exact_mann_whitney_two_sided(sample_a, sample_b):
    """Exact two-sided permutation p-value, including ties."""
    combined = sample_a + sample_b
    ranks = doubled_ranks(combined)
    size_a = len(sample_a)
    size_b = len(sample_b)
    observed_u2 = u_statistic_times_two(sample_a, sample_b)
    expected_u2 = size_a * size_b
    observed_distance = abs(observed_u2 - expected_u2)
    extreme = 0
    total = 0
    rank_offset = size_a * (size_a + 1)
    for indices in itertools.combinations(range(len(combined)), size_a):
        candidate_u2 = sum(ranks[index] for index in indices) - rank_offset
        if abs(candidate_u2 - expected_u2) >= observed_distance:
            extreme += 1
        total += 1
    return observed_u2 / 2.0, extreme / total


def asymptotic_mann_whitney_two_sided(sample_a, sample_b):
    """Tie-corrected normal approximation with continuity correction."""
    size_a = len(sample_a)
    size_b = len(sample_b)
    combined = sample_a + sample_b
    u_value = u_statistic_times_two(sample_a, sample_b) / 2.0
    mean = size_a * size_b / 2.0

    tie_counts = {}
    for value in combined:
        tie_counts[value] = tie_counts.get(value, 0) + 1
    population = len(combined)
    tie_term = sum(count ** 3 - count for count in tie_counts.values())
    variance = size_a * size_b / 12.0 * (
        population + 1 - tie_term / (population * (population - 1))
    )
    if variance == 0:
        return u_value, 1.0
    distance = max(0.0, abs(u_value - mean) - 0.5)
    z_score = distance / math.sqrt(variance)
    p_value = math.erfc(z_score / math.sqrt(2.0))
    return u_value, min(1.0, p_value)


def mann_whitney_two_sided(sample_a, sample_b, exact_max_combinations):
    combinations = math.comb(len(sample_a) + len(sample_b), len(sample_a))
    if combinations <= exact_max_combinations:
        statistic, p_value = exact_mann_whitney_two_sided(sample_a, sample_b)
        return statistic, p_value, "exact", combinations
    statistic, p_value = asymptotic_mann_whitney_two_sided(sample_a, sample_b)
    return statistic, p_value, "asymptotic", combinations


def classify(baseline, reactor, direction, relative_threshold, absolute_threshold):
    baseline_median = statistics.median(baseline)
    reactor_median = statistics.median(reactor)
    if direction == "lower":
        regression = reactor_median - baseline_median
    else:
        regression = baseline_median - reactor_median
    allowed = max(absolute_threshold, abs(baseline_median) * relative_threshold / 100.0)
    breached = regression > allowed
    return baseline_median, reactor_median, regression, allowed, breached


def verdict_for(breached, p_value, alpha):
    if breached and p_value < alpha:
        return "FAIL", FAIL
    if breached:
        return "INCONCLUSIVE", INCONCLUSIVE
    return "PASS", PASS


def read_samples(path, scenario, metric):
    samples = {"baseline": [], "reactor": []}
    with path.open(newline="", encoding="utf-8") as input_file:
        reader = csv.DictReader(input_file)
        required = {"variant", "scenario", metric}
        missing = required.difference(reader.fieldnames or [])
        if missing:
            raise ValueError("missing CSV columns: " + ", ".join(sorted(missing)))
        for line_number, row in enumerate(reader, start=2):
            if row["scenario"] != scenario:
                continue
            variant = row["variant"]
            if variant not in samples:
                continue
            try:
                value = float(row[metric])
            except (TypeError, ValueError) as error:
                raise ValueError(
                    f"line {line_number} has a non-numeric {metric}: {row[metric]!r}"
                ) from error
            if not math.isfinite(value):
                raise ValueError(f"line {line_number} has a non-finite {metric}: {value}")
            samples[variant].append(value)
    for variant, values in samples.items():
        if not values:
            raise ValueError(f"no {variant} samples for scenario {scenario!r}")
    return samples["baseline"], samples["reactor"]


def self_test():
    statistic, p_value = exact_mann_whitney_two_sided([1, 2, 3], [4, 5, 6])
    assert statistic == 0.0
    assert math.isclose(p_value, 0.1)
    statistic, p_value = exact_mann_whitney_two_sided([1, 1], [1, 1])
    assert statistic == 2.0
    assert p_value == 1.0
    assert median_absolute_deviation([1, 2, 100]) == 1
    result = classify([100, 100, 100], [111, 111, 111], "lower", 10, 0)
    assert result[-1] is True
    result = classify([100, 100, 100], [89, 89, 89], "higher", 10, 0)
    assert result[-1] is True
    assert verdict_for(False, 1.0, 0.05) == ("PASS", PASS)
    assert verdict_for(True, 0.1, 0.05) == ("INCONCLUSIVE", INCONCLUSIVE)
    assert verdict_for(True, 0.01, 0.05) == ("FAIL", FAIL)
    print("compare.py self-test: PASS")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, help="results.csv produced by run-idle.sh")
    parser.add_argument("--scenario", help="scenario value to select")
    parser.add_argument("--metric", help="numeric results.csv column to compare")
    parser.add_argument("--direction", choices=("lower", "higher"), help="better direction")
    parser.add_argument(
        "--relative-threshold",
        type=float,
        default=0.0,
        help="allowed regression as percent of the baseline median",
    )
    parser.add_argument(
        "--absolute-threshold",
        type=float,
        default=0.0,
        help="allowed absolute regression in the metric's units",
    )
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument(
        "--exact-max-combinations",
        type=int,
        default=200_000,
        help="largest label permutation count evaluated exactly",
    )
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return args
    missing = [
        name
        for name in ("input", "scenario", "metric", "direction")
        if getattr(args, name) is None
    ]
    if missing:
        parser.error("required arguments: " + ", ".join("--" + name for name in missing))
    if args.relative_threshold < 0 or args.absolute_threshold < 0:
        parser.error("thresholds must be non-negative")
    if not 0 < args.alpha < 1:
        parser.error("--alpha must be between zero and one")
    if args.exact_max_combinations < 1:
        parser.error("--exact-max-combinations must be positive")
    return args


def main():
    args = parse_args()
    if args.self_test:
        self_test()
        return PASS
    try:
        baseline, reactor = read_samples(args.input, args.scenario, args.metric)
    except (OSError, ValueError) as error:
        print(f"compare.py: {error}", file=sys.stderr)
        return USAGE_ERROR

    baseline_median, reactor_median, regression, allowed, breached = classify(
        baseline,
        reactor,
        args.direction,
        args.relative_threshold,
        args.absolute_threshold,
    )
    statistic, p_value, method, combinations = mann_whitney_two_sided(
        baseline, reactor, args.exact_max_combinations
    )
    verdict, exit_code = verdict_for(breached, p_value, args.alpha)

    print(f"verdict={verdict}")
    print(
        f"baseline n={len(baseline)} median={baseline_median:.9g} "
        f"mad={median_absolute_deviation(baseline):.9g}"
    )
    print(
        f"reactor n={len(reactor)} median={reactor_median:.9g} "
        f"mad={median_absolute_deviation(reactor):.9g}"
    )
    print(
        f"direction={args.direction} regression={regression:.9g} "
        f"allowed={allowed:.9g} breached={str(breached).lower()}"
    )
    print(
        f"mann_whitney_u={statistic:.9g} p={p_value:.9g} method={method} "
        f"label_combinations={combinations} alpha={args.alpha:.9g}"
    )
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
