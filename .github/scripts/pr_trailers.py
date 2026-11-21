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

from collections import defaultdict
import re
import shlex
import subprocess
import tempfile
import textwrap
from typing import Dict, List, TextIO

_TRAILER_HEADER = re.compile(r"^[A-Za-z0-9-]+:")
_TRAILER_CONTINUATION = re.compile(r"^[ \t]+\S")
_TRAILER_WIDTH = 72


def write_commit(io: TextIO, title: str, body: str) -> None:
    io.write(title.encode())
    io.write(b"\n\n")
    io.write(body.encode())
    io.flush()


def parse_trailers(title: str, body: str) -> Dict[str, List[str]]:
    trailers = defaultdict(list)
    normalized_body = body.replace("\r\n", "\n").replace("\r", "\n")

    with tempfile.NamedTemporaryFile() as fp:
        write_commit(fp, title, normalized_body)
        cmd = f"git interpret-trailers --trim-empty --parse {fp.name}"
        p = subprocess.run(shlex.split(cmd), capture_output=True)
        fp.close()

    for line in p.stdout.decode().splitlines():
        key, value = line.split(":", 1)
        trailers[key].append(value.strip())

    return trailers


def format_trailers(body: str, width: int = _TRAILER_WIDTH) -> str:
    """Wrap only the trailing Git trailer block using RFC 822 folding.

    The PR description body above the trailing trailer block is returned
    byte-for-byte unchanged so Markdown tables, lists, code fences, and prose
    are never rewritten.
    """
    if not body:
        return body

    lines = body.splitlines(keepends=True)

    # Locate the final non-blank paragraph.
    end = len(lines)
    while end > 0 and lines[end - 1].strip() == "":
        end -= 1
    if end == 0:
        return body

    start = end
    while start > 0 and lines[start - 1].strip() != "":
        start -= 1

    block = lines[start:end]

    # Unfold the block into individual trailers; bail out if any line is not a
    # valid trailer header or indented continuation line.
    entries: List[str] = []
    for raw in block:
        text = raw.rstrip("\r\n")
        if _TRAILER_HEADER.match(text):
            entries.append(text)
        elif _TRAILER_CONTINUATION.match(text) and entries:
            entries[-1] = f"{entries[-1]} {text.strip()}"
        else:
            return body

    if not entries:
        return body

    # Confirm Git itself recognizes this trailing block as trailers.
    if not parse_trailers("Subject", body):
        return body

    # Use LF in the trailer block so folded continuation lines parse cleanly
    # in git interpret-trailers, while keeping body content above untouched.
    has_trailing_newline = block[-1].endswith(("\r\n", "\n"))

    wrapped_entries = [
        textwrap.fill(
            entry,
            width=width,
            subsequent_indent=" ",
            break_long_words=False,
            break_on_hyphens=False,
            replace_whitespace=False,
        )
        for entry in entries
    ]
    new_block = "\n".join(wrapped_entries) + ("\n" if has_trailing_newline else "")
    return "".join(lines[:start]) + new_block + "".join(lines[end:])
