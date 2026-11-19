# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "skills"
    / "hugegraph-deepwiki-skill"
    / "scripts"
    / "deepwiki_mcp.py"
)


def load_mcp_module():
    spec = importlib.util.spec_from_file_location(
        "deepwiki_mcp_under_test", SCRIPT_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


mcp = load_mcp_module()


class TimeoutResponse:
    def readline(self):
        raise TimeoutError()


class PartialTimeoutResponse:
    def __init__(self):
        self.lines = [b'data: {"jsonrpc":"2.0","id":7,\n']

    def readline(self):
        if self.lines:
            return self.lines.pop(0)
        raise TimeoutError()


class DeepWikiMcpTest(unittest.TestCase):
    def test_resolve_repo_accepts_alias_and_full_repo_name(self):
        self.assertEqual("apache/hugegraph", mcp.resolve_repo("hugegraph"))
        self.assertEqual("apache/hugegraph", mcp.resolve_repo("apache/hugegraph"))

    def test_read_sse_response_reports_socket_timeout(self):
        with (
            mock.patch.dict(os.environ, {"DEEPWIKI_MCP_STREAM_TIMEOUT": "1"}),
            self.assertRaisesRegex(mcp.McpError, "timed out waiting for response id 7"),
        ):
            mcp.read_sse_response(TimeoutResponse(), 7)

    def test_read_sse_response_reports_partial_event_timeout(self):
        with (
            mock.patch.dict(os.environ, {"DEEPWIKI_MCP_STREAM_TIMEOUT": "1"}),
            self.assertRaisesRegex(mcp.McpError, "timed out waiting for response id 7"),
        ):
            mcp.read_sse_response(PartialTimeoutResponse(), 7)

    def test_cache_write_failure_returns_fetched_contents(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache_path = Path(tmp_dir) / "apache__hugegraph" / "wiki-contents.md"
            with (
                mock.patch.object(mcp, "contents_cache_path", return_value=cache_path),
                mock.patch.object(
                    mcp, "read_wiki_contents", return_value="fresh wiki"
                ) as read_wiki,
                mock.patch.object(
                    mcp, "write_text_atomic", side_effect=OSError("readonly")
                ),
            ):
                text, path, status = mcp.ensure_cached_contents(
                    object(), "apache/hugegraph"
                )

        self.assertEqual("fresh wiki", text)
        self.assertEqual(cache_path, path)
        self.assertIn("cache write skipped", status)
        read_wiki.assert_called_once()

    def test_bad_cached_contents_are_refetched(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache_path = Path(tmp_dir) / "apache__hugegraph" / "wiki-contents.md"
            cache_path.parent.mkdir(parents=True)
            cache_path.write_bytes(b"\xff\xfe")
            with (
                mock.patch.object(mcp, "contents_cache_path", return_value=cache_path),
                mock.patch.object(
                    mcp, "read_wiki_contents", return_value="fresh wiki"
                ) as read_wiki,
            ):
                text, path, status = mcp.ensure_cached_contents(
                    object(), "apache/hugegraph"
                )

            self.assertEqual("fresh wiki", text)
            self.assertEqual(cache_path, path)
            self.assertEqual("refreshed from DeepWiki", status)
            self.assertEqual("fresh wiki", cache_path.read_text(encoding="utf-8"))
            read_wiki.assert_called_once()

    def test_cached_context_selects_scored_non_overlapping_snippets(self):
        lines = ["overview"] * 80
        lines[5] = "Gremlin traversal examples explain graph query execution."
        lines[50] = "Gremlin traversal cache context covers answer routing."

        matches = mcp.search_cached_context("\n".join(lines), "gremlin traversal", 2)

        self.assertEqual(2, len(matches))
        self.assertGreater(matches[0][0], 0)
        self.assertGreater(matches[1][0], 0)
        self.assertFalse(
            matches[0][1] <= matches[1][2] and matches[0][2] >= matches[1][1]
        )


if __name__ == "__main__":
    unittest.main()
