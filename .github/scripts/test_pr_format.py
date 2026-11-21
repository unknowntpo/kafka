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

import importlib.util
import io
import json
from pathlib import Path
import runpy
import subprocess
import sys
import unittest
from unittest.mock import patch

SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import pr_trailers  # noqa: E402


def _load_script(name: str):
    path = SCRIPTS_DIR / name
    spec = importlib.util.spec_from_file_location(name.replace("-", "_").removesuffix(".py"), path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


pr_reviewers_trailer = _load_script("pr-reviewers-trailer.py")


class FormatTrailersTest(unittest.TestCase):
    def _assert_idempotent_and_parse(self, body: str, expected: str, expected_trailers=None):
        formatted = pr_trailers.format_trailers(body)
        self.assertEqual(expected, formatted)
        self.assertEqual(formatted, pr_trailers.format_trailers(formatted))
        if expected_trailers is not None:
            self.assertEqual(expected_trailers, pr_trailers.parse_trailers("KAFKA-21579: Test", formatted))

    def test_body_markdown_never_modified(self):
        cases = (
            # Markdown table (short and long)
            "| Case | Before | After | Notes |\n"
            "|---|---:|---:|---|\n"
            "| `SelfManagedOffsetLifecycleIntegrationTest` | 102.714s | 51.617s | long table row exceeding 72 columns |\n",
            # Indented & nested lists and '+' bullets (KAFKA-21205)
            "Example list:\n - first item\n - second item\n\n+ plus item\n  - nested child exceeding seventy two characters in a single line here\n",
            # Fenced code block with blank lines and long lines (KAFKA-21205)
            "```java\nfirst command\n\nsecond command\nthird command\n"
            "System.out.println(\"a very long line inside a fenced code block that exceeds 72 columns\");\n```\n",
            # Blockquotes, HTML <details>, long heading, long prose
            "### A very long markdown heading that exceeds seventy two characters in total length\n\n"
            "> quote line 1\n> quote line 2\n\n"
            "<details>\n<summary>Details</summary>\n\nsome body text\n</details>\n\n"
            "A long paragraph exceeding seventy two characters that should be left completely untouched by the script.\n",
            # Non-trailer paragraphs at end of body
            "Note: this starts like a trailer\nbut continues on column 0.\n",
            "```\nKey: value inside code fence\n```\n",
            "",
            "\n",
        )
        for body in cases:
            with self.subTest(body=body[:40]):
                self._assert_idempotent_and_parse(body, body)

    def test_adjacent_trailers_stay_separate_and_wrap_with_indent(self):
        body = (
            "Summary with a table:\n\n"
            "| Col A | Col B |\n|---|---|\n| 1 | 2 |\n\n"
            "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\n"
            "Generated-by: Some Long Model Name (Version 1.2.3) with Extra Attribution Details\n"
            "Reviewers: Ken Huang <s7133700@gmail.com>, Federico Valeri <fedevaleri@gmail.com>\n"
        )
        expected = (
            "Summary with a table:\n\n"
            "| Col A | Col B |\n|---|---|\n| 1 | 2 |\n\n"
            "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\n"
            "Generated-by: Some Long Model Name (Version 1.2.3) with Extra\n"
            " Attribution Details\n"
            "Reviewers: Ken Huang <s7133700@gmail.com>, Federico Valeri\n"
            " <fedevaleri@gmail.com>\n"
        )
        self._assert_idempotent_and_parse(
            body,
            expected,
            expected_trailers={
                "Co-Authored-By": ["Claude Opus 5.5 <noreply@anthropic.com>"],
                "Generated-by": ["Some Long Model Name (Version 1.2.3) with Extra Attribution Details"],
                "Reviewers": ["Ken Huang <s7133700@gmail.com>, Federico Valeri <fedevaleri@gmail.com>"],
            },
        )

    def test_reviewers_before_other_trailer(self):
        body = (
            "Fix bug.\n\n"
            "Reviewers: Ada Lovelace <ada@example.com>, Grace Hopper <grace@example.com>\n"
            "Generated-by: Codex (GPT-6)\n"
        )
        expected = (
            "Fix bug.\n\n"
            "Reviewers: Ada Lovelace <ada@example.com>, Grace Hopper\n"
            " <grace@example.com>\n"
            "Generated-by: Codex (GPT-6)\n"
        )
        self._assert_idempotent_and_parse(
            body,
            expected,
            expected_trailers={
                "Reviewers": ["Ada Lovelace <ada@example.com>, Grace Hopper <grace@example.com>"],
                "Generated-by": ["Codex (GPT-6)"],
            },
        )

    def test_preserves_crlf_body_while_wrapping_trailer(self):
        body = (
            "| A | B |\r\n|---|---|\r\n| 1 | 2 |\r\n\r\n"
            "Reviewers: Ken Huang <s7133700@gmail.com>, Federico Valeri <fedevaleri@gmail.com>\r\n"
        )
        expected = (
            "| A | B |\r\n|---|---|\r\n| 1 | 2 |\r\n\r\n"
            "Reviewers: Ken Huang <s7133700@gmail.com>, Federico Valeri\n"
            " <fedevaleri@gmail.com>\n"
        )
        self._assert_idempotent_and_parse(
            body,
            expected,
            expected_trailers={
                "Reviewers": ["Ken Huang <s7133700@gmail.com>, Federico Valeri <fedevaleri@gmail.com>"],
            },
        )

    def test_update_reviewers_trailer_wraps_and_preserves_markdown(self):
        body = (
            "| A | B |\n|---|---|\n| 1 | 2 |\n\n"
            "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\n"
            "Reviewers: Ken Huang <s7133700@gmail.com>\n"
        )
        updated = pr_reviewers_trailer.update_reviewers_trailer(
            body,
            "Reviewers: Ken Huang <s7133700@gmail.com>, Federico Valeri <fedevaleri@gmail.com>",
        )
        expected = (
            "| A | B |\n|---|---|\n| 1 | 2 |\n\n"
            "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\n"
            "Reviewers: Ken Huang <s7133700@gmail.com>, Federico Valeri\n"
            " <fedevaleri@gmail.com>\n"
        )
        self.assertEqual(expected, updated)
        self.assertEqual(
            {
                "Co-Authored-By": ["Claude Opus 5.5 <noreply@anthropic.com>"],
                "Reviewers": ["Ken Huang <s7133700@gmail.com>, Federico Valeri <fedevaleri@gmail.com>"],
            },
            pr_trailers.parse_trailers("KAFKA-21579: Test", updated),
        )

    def test_cli_skips_edit_when_trailers_unchanged(self):
        script = SCRIPTS_DIR / "pr-format.py"
        cases = (
            # Body has long prose and table, no trailer change -> must NOT call gh pr edit
            (
                "| A | B |\n|---|---|\n| 1 | 2 |\n\n"
                "Long prose line exceeding seventy two characters should not trigger an edit.\n",
                None,
            ),
            # Long Reviewers trailer -> calls gh pr edit with wrapped trailer only
            (
                "| A | B |\n|---|---|\n| 1 | 2 |\n\n"
                "Reviewers: Ken Huang <s7133700@gmail.com>, Federico Valeri <fedevaleri@gmail.com>\n",
                "| A | B |\n|---|---|\n| 1 | 2 |\n\n"
                "Reviewers: Ken Huang <s7133700@gmail.com>, Federico Valeri\n"
                " <fedevaleri@gmail.com>\n",
            ),
        )
        real_run = subprocess.run

        for body, expected_edit in cases:
            with self.subTest(will_edit=expected_edit is not None):
                gh_calls = []
                edited_bodies = []

                def fake_run(cmd, *args, **kwargs):
                    if cmd[0] == "gh":
                        gh_calls.append(cmd)
                        if cmd[:3] == ["gh", "pr", "view"]:
                            return subprocess.CompletedProcess(
                                args=cmd,
                                returncode=0,
                                stdout=json.dumps({
                                    "title": "KAFKA-21579 Preserve PR body",
                                    "body": body,
                                    "reviews": [],
                                }).encode(),
                                stderr=b"",
                            )
                        if cmd[:3] == ["gh", "pr", "edit"] and "--body-file" in cmd:
                            body_file = cmd[cmd.index("--body-file") + 1]
                            edited_bodies.append(Path(body_file).read_text(encoding="utf-8"))
                        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout=b"", stderr=b"")
                    return real_run(cmd, *args, **kwargs)

                with (
                    patch.dict("os.environ", {"PR_NUMBER": "24300", "GITHUB_ACTIONS": "true"}, clear=True),
                    patch("subprocess.run", side_effect=fake_run),
                    patch("logging.getLogger"),
                    patch("sys.stdout", new_callable=io.StringIO),
                ):
                    with self.assertRaises(SystemExit) as result:
                        runpy.run_path(str(script), run_name="__main__")
                    self.assertEqual(0, result.exception.code)

                self.assertEqual(gh_calls[0], ["gh", "pr", "view", "24300", "--json", "title,body,reviews"])
                if expected_edit is None:
                    self.assertEqual(1, len(gh_calls))
                    self.assertEqual([], edited_bodies)
                else:
                    self.assertEqual(2, len(gh_calls))
                    self.assertEqual(gh_calls[1][:4], ["gh", "pr", "edit", "24300"])
                    self.assertEqual([expected_edit], edited_bodies)


if __name__ == "__main__":
    unittest.main()
