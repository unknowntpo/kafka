#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Prepare isolated source/runtime copies, then one same-worker paired workload.

Never changes the Jenkins checkout or an existing broker. Only the fixed baseline
is fetched from the user's fork. No push, job submission or automatic retry.
Partial receipts survive failures.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile

BASELINE = 'd3c6d866adbdbb15a4c2f5223707e2ba926770fc'
RECORDS = 70_000_000
PROFILES = {'formal': {'records': RECORDS, 'pairs': 5},
            'smoke': {'records': 10_000, 'pairs': 1}}


def terminate(signum, frame):
    raise SystemExit(128 + signum)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--artifacts', type=Path, required=True)
    parser.add_argument('--profile', choices=PROFILES, default='formal')
    args = parser.parse_args()
    profile = PROFILES[args.profile]
    args.artifacts.mkdir(parents=True, exist_ok=False)
    (args.artifacts / 'entry.pid').write_text(str(os.getpid()))
    signal.signal(signal.SIGTERM, terminate)
    repository = args.repository.resolve()
    source = repository / 'benchmarks/contract-guided'
    env = dict(os.environ, LANG='C.UTF-8', LC_ALL='C.UTF-8',
               JAVA_TOOL_OPTIONS='-Dfile.encoding=UTF-8')
    java = Path(shutil.which('java')).resolve()
    version = subprocess.check_output([str(java), '-version'], stderr=subprocess.STDOUT, text=True)
    if not re.search(r'version "17[.\"]', version):
        raise RuntimeError('This comparison requires Java 17 for both variants')
    if shutil.disk_usage(args.artifacts).free < 35 * 1024**3:
        raise RuntimeError('Need at least 35 GiB free for 17.92 GB payload and isolated builds')
    revision = subprocess.check_output(['git', '-C', str(repository), 'rev-parse', 'HEAD'], text=True).strip()
    dirty = subprocess.check_output(['git', '-C', str(repository), 'status', '--porcelain',
                                     '--untracked-files=no'], text=True).strip()
    if dirty:
        raise RuntimeError('Jenkins source checkout has tracked edits')
    build_root = Path(tempfile.mkdtemp(prefix='kip1371-build-', dir=str(args.artifacts.parent)))
    (args.artifacts / 'preparation.json').write_text(json.dumps({
        'baseline': BASELINE, 'candidate': revision, 'java': version,
        'build_root': str(build_root), 'profile': args.profile, **profile,
        'host_exclusive': False, 'scope': 'healthy subscribed auto-commit throughput; not idle',
    }, indent=2))

    def run(label, command, timeout):
        print('START ' + label, flush=True)
        with (args.artifacts / (label + '.log')).open('w') as log:
            child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                     env=env, start_new_session=True)
            try:
                code = child.wait(timeout=timeout)
            except BaseException:
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=90)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait()
                raise
        if code:
            raise RuntimeError('%s exited %s; see its log' % (label, code))

    try:
        for role, commit in [('baseline', BASELINE), ('candidate', revision)]:
            checkout = build_root / role
            run(role + '-clone', ['git', 'clone', '--no-hardlinks', '--no-checkout',
                                  str(repository), str(checkout)], 300)
            if role == 'baseline':
                # The job's checkout is shallow: fetch only this immutable baseline,
                # into our disposable clone, never into the Jenkins source checkout.
                run('baseline-fetch', ['git', '-C', str(checkout), 'fetch', '--depth=1', '--no-tags',
                                       'https://github.com/unknowntpo/kafka.git', BASELINE], 300)
            run(role + '-checkout', ['git', '-C', str(checkout), 'checkout', '--detach', commit], 120)
            run(role + '-build', [str(checkout / 'gradlew'), '-p', str(checkout), '--no-daemon',
                                  '--max-workers=2', '-Dorg.gradle.jvmargs=-Xmx2g -Dfile.encoding=UTF-8',
                                  '-I', str(source / 'runtime-classpath.gradle'),
                                  '-PcontractGuidedOutput=' + str(build_root / (role + '-runtime')),
                                  '-PcontractGuidedBroker=' + ('true' if role == 'baseline' else 'false'),
                                  'contractGuidedRuntime'], 3600)
        # Hold the broker runtime fixed at the control revision in this ablation.
        shutil.copyfile(build_root / 'baseline-runtime/broker-classpath.txt',
                        build_root / 'candidate-runtime/broker-classpath.txt')
        # Compilation is finished before either timing population starts.
        run('paired-throughput', [sys.executable, str(source / 'run-throughput.py'),
                                 '--java', str(java), '--records', str(profile['records']),
                                 '--pairs', str(profile['pairs']),
                                 '--baseline-worktree', str(build_root / 'baseline'),
                                 '--candidate-worktree', str(build_root / 'candidate'),
                                 '--baseline-runtime', str(build_root / 'baseline-runtime'),
                                 '--candidate-runtime', str(build_root / 'candidate-runtime'),
                                 '--artifact-parent', str(args.artifacts), '--remove-owned-data',
                                 '--profile', '--profile-records', str(profile['records'])], 5400)
        summaries = list(args.artifacts.glob('kip1371-throughput-*/summary.json'))
        if len(summaries) != 1:
            raise RuntimeError('Missing unambiguous paired summary')
        summary = json.loads(summaries[0].read_text())
        for role in ('baseline', 'candidate'):
            run(role + '-profile-summary', [sys.executable, str(source / 'summarize-jfr.py'),
                                            str(summaries[0].parent / (role + '.jfr')),
                                            '--jfr', str(java.with_name('jfr')),
                                            '--output', str(summaries[0].parent / ('safe-' + role))], 180)
        if args.profile == 'formal' and summary['gate'] != 'pass':
            raise RuntimeError('Benchmark not accepted: ' + summary['gate'])
        # Small runs validate plumbing, never formal performance acceptance.
        rows = json.loads((summaries[0].parent / 'results.json').read_text())
        if len(rows) != profile['pairs'] * 2 or any(row['records'] != profile['records'] for row in rows):
            raise RuntimeError('Incomplete paired receipt')
        (args.artifacts / 'completed.json').write_text(json.dumps({
            'profile': args.profile, 'candidate': revision,
            'status': 'formal-accepted' if args.profile == 'formal' else 'smoke-validated',
            'performance_gate': summary['gate'], 'timed_jvms': len(rows),
        }, indent=2))
    finally:
        # Only the fresh mkdtemp directory owned by this invocation; receipts live elsewhere.
        shutil.rmtree(build_root)
        (args.artifacts / 'entry.pid').unlink(missing_ok=True)


if __name__ == '__main__':
    main()
