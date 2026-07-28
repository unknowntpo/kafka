# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Rank the slowest JUnit tests, classes, and modules.

The "JUnit tests" jobs on trunk are bounded by a three hour timeout (see build.yml). This script
finds where that time actually goes by aggregating the per-test durations recorded in the JUnit
XML reports.

Three input sources are supported and may be combined:

  * a local directory of JUnit XML (what "./gradlew test" writes to build/junit-xml)
  * artifact ZIP files downloaded from a workflow run ("junit-xml-<java>-<flaky>-<new>")
  * the GitHub Actions REST API, which downloads those artifacts for the most recent runs

Examples:

    # Local run. GITHUB_ACTIONS is what makes the build collect the reports into build/junit-xml;
    # without it, point --path at the repository root and the per-project reports are found instead.
    GITHUB_ACTIONS=true ./gradlew test -Pkafka.test.xml.output.dir=local
    python .github/scripts/slowest_tests.py --path build/junit-xml

    # Last three trunk runs of ci.yml (needs a token with actions:read)
    export GITHUB_TOKEN=...
    python .github/scripts/slowest_tests.py --gh-repo apache/kafka --runs 3

    # Artifact ZIPs downloaded by hand from the run page
    python .github/scripts/slowest_tests.py --zip junit-xml-25-noflaky-nonew.zip

Methodology
-----------

What the numbers are. Every duration comes from the "time" attribute of a <testcase> element,
which JUnit records for a single execution of a single test method. It covers the method plus
its per-method fixtures (@BeforeEach/@AfterEach), and it excludes the JVM fork startup, the
per-class fixtures, Gradle's own overhead, and compilation. Summed test time is therefore a
lower bound on the wall clock of a test job, and the two should not be compared directly.

Total versus wall clock. The test task runs with -PmaxParallelForks=4 in CI, so four test
classes execute at once and the summed test time of a job is roughly four times its wall clock.
The three hour timeout in build.yml applies to wall clock, which is why --gh-repo mode also
prints the measured duration of each "JUnit tests" job: use those numbers to see how close a
run came to the timeout, and the rankings below to see what to attack.

Which grouping to read. Gradle schedules work at class granularity, so a class is what actually
occupies a fork for a contiguous stretch. --group class is the right view for "what makes the
job long", and it is the view where a single very slow class shows up as a critical path that
extra forks cannot shorten. --group method finds individual pathological tests, and
--group module shows how the cost is distributed across the build.

Repeated executions. The same test can run more than once in the data: the Develocity test
retry plugin re-runs failures (-PmaxTestRetries=3 in the flaky variation), ClusterTests are
repeated on pull requests (-Pkafka.cluster.test.repeat=3), parameterized tests report one
<testcase> per invocation, and when several artifacts or runs are loaded each contributes its
own copy. Every execution is summed into the total and counted in the "Runs" column, so a class
whose total is large because it runs 250 cheap tests is distinguishable from one that runs a
single ten minute test. "Mean" is the total divided by the number of executions and "Max" is
the slowest single execution.

Skipped tests are dropped by default. They report a zero duration, and counting them only
dilutes the mean. Pass --include-skipped to keep them.

Reproducing a measurement locally. The following mirrors the "noflaky-nonew" matrix variation,
which is the variation that runs the bulk of the suite. It excludes tests tagged @Flaky, and
with no test catalog on disk the "new test" selection is empty, exactly as in CI:

    GITHUB_ACTIONS=true ./gradlew :core:test \
      --build-cache --continue --no-scan \
      -PmaxParallelForks=4 -PcommitId=xxxxxxxxxxxxxxxx \
      -Pkafka.test.run.flaky=false -Pkafka.test.xml.output.dir=local \
      -x spotbugsMain -x spotbugsTest
    python .github/scripts/slowest_tests.py --path build/junit-xml --top 10 --group class

Running one module at a time keeps a failing or hanging module from costing the whole
measurement, and the reports accumulate under build/junit-xml, so the rankings can be produced
from whatever has completed so far. GITHUB_ACTIONS is what enables the copyTestXml task that
collects the reports there.

Caveats when reading a local measurement. Durations scale with the machine: an integration test
that waits on a fixed condition changes little, while a CPU-bound test tracks the core count, so
a local ranking is more trustworthy than the local absolute numbers. Numbers taken from CI
artifacts do not have that problem and should be preferred when they are available. Tests that
failed still report the time they burned before failing, which is intentional, since a test that
times out after two minutes costs the job those two minutes whether or not it passed.
"""

import argparse
import collections
import dataclasses
import io
import json
import logging
import os
import pathlib
import sys
import xml.etree.ElementTree
import zipfile
from typing import Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

JUNIT_XML_ARTIFACT_PREFIX = "junit-xml"
TEST_JOB_NAME_PREFIX = "JUnit tests"
GITHUB_API = "https://api.github.com"


@dataclasses.dataclass(frozen=True)
class TestExecution:
    """A single <testcase> element from a JUnit XML report."""
    module: str
    job: str
    class_name: str
    test_name: str
    time: float
    skipped: bool
    source: str


@dataclasses.dataclass
class Aggregate:
    key: str
    total_time: float = 0.0
    max_time: float = 0.0
    executions: int = 0
    distinct_tests: set = dataclasses.field(default_factory=set)
    sources: set = dataclasses.field(default_factory=set)

    def add(self, execution: TestExecution) -> None:
        self.total_time += execution.time
        self.max_time = max(self.max_time, execution.time)
        self.executions += 1
        self.distinct_tests.add((execution.class_name, execution.test_name))
        self.sources.add(execution.source)

    @property
    def mean_time(self) -> float:
        return self.total_time / self.executions if self.executions else 0.0


def pretty_time_duration(seconds: float) -> str:
    """Format a duration the same way junit.py does, e.g. 1h2m3s."""
    time_min, time_sec = divmod(int(seconds), 60)
    time_hour, time_min = divmod(time_min, 60)
    time_fmt = ""
    if time_hour > 0:
        time_fmt += f"{time_hour}h"
    if time_min > 0:
        time_fmt += f"{time_min}m"
    time_fmt += f"{time_sec}s"
    return time_fmt


def split_report_path(base_path: str, report_path: str) -> Tuple[str, str]:
    """
    Extract the module and test job from a report path. Report paths in build/junit-xml and in
    the artifacts built from it look like

        <base_path>/module[/sub-module]/[test-job]/TEST-class.method.xml

    A local build that did not run the copyTestXml task instead leaves reports in each
    sub-project, which is handled as a special case

        <base_path>/module[/sub-module]/build/test-results/test/TEST-class.method.xml

    Returns a tuple of (module, job). Paths that are too short to carry both fall back to
    whatever is available so that ad-hoc directories still parse.
    """
    rel_report_path = os.path.relpath(report_path, base_path)
    path_segments = pathlib.Path(rel_report_path).parts
    if "build" in path_segments[:-1]:
        build_at = path_segments.index("build")
        module = os.path.join(*path_segments[0:build_at]) if build_at > 0 else "unknown"
        return module, path_segments[-2]
    if len(path_segments) >= 3:
        return os.path.join(*path_segments[0:-2]), path_segments[-2]
    elif len(path_segments) == 2:
        return path_segments[0], "unknown"
    else:
        return "unknown", "unknown"


def parse_report(fp, module: str, job: str, source: str) -> Iterable[TestExecution]:
    """Yield a TestExecution for every <testcase> in a JUnit XML report."""
    class_name = None
    test_name = None
    test_time = 0.0
    skipped = False
    for event, elem in xml.etree.ElementTree.iterparse(fp, events=["start", "end"]):
        if event == "start":
            if elem.tag == "testcase":
                class_name = elem.get("classname") or "unknown"
                test_name = elem.get("name") or "unknown"
                test_time = float(elem.get("time", 0.0))
                skipped = False
            elif elem.tag == "skipped":
                skipped = True
        elif event == "end" and elem.tag == "testcase":
            yield TestExecution(module, job, class_name, test_name, test_time, skipped, source)
            class_name = None


def read_directory(path: str, source: Optional[str] = None) -> Iterable[TestExecution]:
    reports = list(pathlib.Path(path).rglob("*.xml"))
    logger.debug(f"Found {len(reports)} XML reports under {path}")
    for report in reports:
        module, job = split_report_path(path, str(report))
        with open(report, "r") as fp:
            try:
                yield from parse_report(fp, module, job, source or job)
            except xml.etree.ElementTree.ParseError as e:
                logger.warning(f"Skipping malformed report {report}: {e}")


def read_zip(zip_path: str, source: Optional[str] = None) -> Iterable[TestExecution]:
    """
    Read a junit-xml artifact ZIP. The archive is rooted at the contents of build/junit-xml,
    so entries look like module/job/TEST-class.xml.
    """
    label = source or pathlib.Path(zip_path).stem
    with zipfile.ZipFile(zip_path) as zf:
        names = [n for n in zf.namelist() if n.endswith(".xml")]
        logger.debug(f"Found {len(names)} XML reports in {zip_path}")
        for name in names:
            module, job = split_report_path(".", name)
            with zf.open(name) as raw:
                try:
                    yield from parse_report(io.TextIOWrapper(raw, encoding="utf-8"), module, job, label)
                except xml.etree.ElementTree.ParseError as e:
                    logger.warning(f"Skipping malformed report {name} in {zip_path}: {e}")


class GitHubActions:
    """Minimal GitHub Actions REST client. Only needs the actions:read scope."""

    def __init__(self, repo: str, token: Optional[str]):
        import requests  # imported lazily so the local and ZIP modes need no dependencies

        self.repo = repo
        self.session = requests.Session()
        headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        else:
            logger.warning("No GitHub token given. Artifact downloads will fail without one.")
        self.session.headers.update(headers)

    def _get(self, url: str, **kwargs):
        if url.startswith("/"):
            url = f"{GITHUB_API}{url}"
        response = self.session.get(url, timeout=60, **kwargs)
        response.raise_for_status()
        return response

    def recent_runs(self, workflow: str, branch: str, count: int, event: Optional[str]) -> List[dict]:
        params = {"branch": branch, "status": "completed", "per_page": max(count * 2, 10)}
        if event:
            params["event"] = event
        runs = self._get(f"/repos/{self.repo}/actions/workflows/{workflow}/runs", params=params).json()["workflow_runs"]
        return runs[:count]

    def run(self, run_id: int) -> dict:
        return self._get(f"/repos/{self.repo}/actions/runs/{run_id}").json()

    def jobs(self, run_id: int) -> List[dict]:
        return self._get(f"/repos/{self.repo}/actions/runs/{run_id}/jobs", params={"per_page": 100}).json()["jobs"]

    def junit_artifacts(self, run_id: int) -> List[dict]:
        artifacts = self._get(f"/repos/{self.repo}/actions/runs/{run_id}/artifacts",
                              params={"per_page": 100}).json()["artifacts"]
        return [a for a in artifacts if a["name"].startswith(JUNIT_XML_ARTIFACT_PREFIX) and not a["expired"]]

    def download_artifact(self, artifact: dict, out_dir: str) -> str:
        out_path = os.path.join(out_dir, f"{artifact['id']}-{artifact['name']}.zip")
        if os.path.exists(out_path):
            logger.debug(f"Using cached artifact {out_path}")
            return out_path
        logger.debug(f"Downloading artifact {artifact['name']} ({artifact['size_in_bytes'] / 1e6:.1f} MB)")
        response = self._get(artifact["archive_download_url"], stream=True)
        os.makedirs(out_dir, exist_ok=True)
        with open(out_path, "wb") as fp:
            for chunk in response.iter_content(chunk_size=1 << 20):
                fp.write(chunk)
        return out_path


def job_durations(jobs: List[dict]) -> List[Tuple[str, float]]:
    """Wall clock seconds for each test job, which is what the three hour timeout applies to."""
    from datetime import datetime

    durations = []
    for job in jobs:
        if not job["name"].startswith(TEST_JOB_NAME_PREFIX):
            continue
        if not job.get("started_at") or not job.get("completed_at"):
            continue
        started = datetime.fromisoformat(job["started_at"].replace("Z", "+00:00"))
        completed = datetime.fromisoformat(job["completed_at"].replace("Z", "+00:00"))
        durations.append((job["name"], (completed - started).total_seconds()))
    return sorted(durations, key=lambda pair: pair[1], reverse=True)


def collect_from_github(args) -> Tuple[List[TestExecution], List[Tuple[str, str, float]]]:
    gh = GitHubActions(args.gh_repo, args.token)
    if args.run_id:
        runs = [gh.run(run_id) for run_id in args.run_id]
    else:
        runs = gh.recent_runs(args.workflow, args.branch, args.runs, args.event)
    if not runs:
        logger.error("No completed workflow runs matched the given filters.")
        return [], []

    executions: List[TestExecution] = []
    wall_clock: List[Tuple[str, str, float]] = []
    for run in runs:
        run_id = run["id"]
        label = f"{run_id}@{run['head_sha'][:7]}"
        logger.info(f"Run {run_id} ({run['head_branch']}, {run['created_at']}): {run['html_url']}")
        try:
            for job_name, seconds in job_durations(gh.jobs(run_id)):
                wall_clock.append((label, job_name, seconds))
        except Exception as e:
            logger.warning(f"Could not read jobs for run {run_id}: {e}")

        artifacts = gh.junit_artifacts(run_id)
        if not artifacts:
            logger.warning(f"Run {run_id} has no unexpired {JUNIT_XML_ARTIFACT_PREFIX}-* artifacts.")
        for artifact in artifacts:
            zip_path = gh.download_artifact(artifact, args.cache_dir)
            executions.extend(read_zip(zip_path, source=f"{label}/{artifact['name']}"))
    return executions, wall_clock


def aggregate(executions: Iterable[TestExecution], group: str) -> Dict[str, Aggregate]:
    aggregates: Dict[str, Aggregate] = collections.OrderedDict()
    for execution in executions:
        if group == "method":
            key = f"{execution.class_name}#{execution.test_name}"
        elif group == "class":
            key = execution.class_name
        else:
            key = execution.module
        if key not in aggregates:
            aggregates[key] = Aggregate(key)
        aggregates[key].add(execution)
    return aggregates


def format_table(rows: List[List[str]], headers: List[str], markdown: bool) -> str:
    if markdown:
        lines = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
        lines.extend("| " + " | ".join(row) + " |" for row in rows)
        return "\n".join(lines)

    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    out = ["  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))]
    out.append("  ".join("-" * widths[i] for i in range(len(headers))))
    for row in rows:
        out.append("  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)))
    return "\n".join(out)


def report(aggregates: Dict[str, Aggregate], total_time: float, args) -> str:
    sort_key = {
        "total": lambda a: a.total_time,
        "mean": lambda a: a.mean_time,
        "max": lambda a: a.max_time,
    }[args.sort]
    ranked = sorted(aggregates.values(), key=sort_key, reverse=True)[:args.top]

    if args.format == "json":
        return json.dumps([{
            "rank": i + 1,
            "name": a.key,
            "total_seconds": round(a.total_time, 3),
            "mean_seconds": round(a.mean_time, 3),
            "max_seconds": round(a.max_time, 3),
            "executions": a.executions,
            "distinct_tests": len(a.distinct_tests),
            "share_of_total": round(a.total_time / total_time, 6) if total_time else 0.0,
        } for i, a in enumerate(ranked)], indent=2)

    headers = ["#", args.group.capitalize(), "Total", "Share", "Runs", "Mean", "Max"]
    rows = []
    for i, a in enumerate(ranked):
        share = f"{100 * a.total_time / total_time:.1f}%" if total_time else "n/a"
        rows.append([
            str(i + 1),
            a.key,
            pretty_time_duration(a.total_time),
            share,
            str(a.executions),
            f"{a.mean_time:.1f}s",
            f"{a.max_time:.1f}s",
        ])
    return format_table(rows, headers, args.format == "markdown")


def main(args) -> int:
    executions: List[TestExecution] = []
    wall_clock: List[Tuple[str, str, float]] = []

    for path in args.path:
        executions.extend(read_directory(path))
    for zip_path in args.zip:
        executions.extend(read_zip(zip_path))
    if args.gh_repo:
        gh_executions, wall_clock = collect_from_github(args)
        executions.extend(gh_executions)

    if not executions:
        logger.error("No test executions were found. Check --path, --zip, or the GitHub options.")
        return 1

    if not args.include_skipped:
        executions = [e for e in executions if not e.skipped]

    total_time = sum(e.time for e in executions)
    aggregates = aggregate(executions, args.group)

    lines = []
    if args.format != "json":
        distinct = len({(e.class_name, e.test_name) for e in executions})
        lines.append(f"Parsed {len(executions)} test executions ({distinct} distinct tests) "
                     f"totalling {pretty_time_duration(total_time)} of test time.")
        if wall_clock:
            lines.append("")
            lines.append("Test job wall clock (three hour timeout applies here):")
            for label, job_name, seconds in sorted(wall_clock, key=lambda t: t[2], reverse=True):
                lines.append(f"  {pretty_time_duration(seconds):>8}  {job_name}  [{label}]")
        lines.append("")
        lines.append(f"Top {args.top} by {args.sort} time, grouped by {args.group}:")
        lines.append("")
    lines.append(report(aggregates, total_time, args))
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Rank the slowest JUnit tests from local reports or GitHub Actions artifacts.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__)
    parser.add_argument("--path", action="append", default=[],
                        help="Directory of JUnit XML reports, e.g. build/junit-xml. May be repeated.")
    parser.add_argument("--zip", action="append", default=[],
                        help="A junit-xml artifact ZIP. May be repeated.")
    parser.add_argument("--gh-repo",
                        help="Repository to pull artifacts from, e.g. apache/kafka.")
    parser.add_argument("--workflow", default="ci.yml",
                        help="Workflow file to read runs from (default: ci.yml).")
    parser.add_argument("--branch", default="trunk",
                        help="Branch to read runs from (default: trunk).")
    parser.add_argument("--event", default="push",
                        help="Only consider runs triggered by this event. Pass an empty string for any.")
    parser.add_argument("--runs", type=int, default=3,
                        help="How many recent runs to download (default: 3).")
    parser.add_argument("--run-id", action="append", type=int, default=[],
                        help="Analyze specific workflow run IDs instead of the most recent ones.")
    parser.add_argument("--cache-dir", default=".junit-artifacts",
                        help="Where downloaded artifacts are kept (default: .junit-artifacts).")
    parser.add_argument("--token", default=os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN"),
                        help="GitHub token. Defaults to $GITHUB_TOKEN or $GH_TOKEN.")
    parser.add_argument("--group", choices=["method", "class", "module"], default="method",
                        help="What to aggregate durations by (default: method).")
    parser.add_argument("--sort", choices=["total", "mean", "max"], default="total",
                        help="Rank by summed, mean, or slowest single execution (default: total).")
    parser.add_argument("--top", type=int, default=10,
                        help="How many rows to print (default: 10).")
    parser.add_argument("--format", choices=["text", "markdown", "json"], default="text",
                        help="Output format (default: text).")
    parser.add_argument("--include-skipped", action="store_true",
                        help="Include skipped tests, which normally have a zero duration.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging on stderr.")

    parsed = parser.parse_args()
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG if parsed.verbose else logging.INFO,
                        format="%(levelname)s %(message)s")
    if not parsed.path and not parsed.zip and not parsed.gh_repo:
        parser.error("Give at least one of --path, --zip, or --gh-repo.")
    if parsed.event == "":
        parsed.event = None
    sys.exit(main(parsed))
