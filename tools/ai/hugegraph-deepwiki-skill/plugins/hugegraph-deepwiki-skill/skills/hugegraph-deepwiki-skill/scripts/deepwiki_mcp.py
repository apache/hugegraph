#!/usr/bin/env python3
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
"""Small DeepWiki MCP client for repository-scoped Q&A."""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Optional


DEFAULT_ENDPOINT = "https://mcp.deepwiki.com/mcp"
CLIENT_NAME = "hugegraph-deepwiki-skill"
SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
PLUGIN_MANIFEST_PATH = SKILL_DIR.parent.parent / ".codex-plugin" / "plugin.json"
REPOS_PATH = SKILL_DIR / "references" / "repos.json"
CLIENT_VERSION_FALLBACK = "0.1.4"
CONTEXT_WINDOW_SIZE = 30
CONTEXT_STRIDE = 10
STOPWORDS = {
    "a",
    "an",
    "and",
    "apache",
    "are",
    "as",
    "for",
    "hugegraph",
    "how",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "the",
    "to",
    "used",
    "what",
    "where",
    "which",
    "why",
}


class McpError(RuntimeError):
    pass


def env_float(name: str, default: float) -> float:
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    try:
        return float(raw_value)
    except ValueError as exc:
        raise McpError(f"{name} must be a number, got {raw_value!r}.") from exc


def stream_timeout_seconds() -> float:
    return max(1.0, env_float("DEEPWIKI_MCP_STREAM_TIMEOUT", 120.0))


def load_client_version() -> str:
    try:
        parsed = json.loads(PLUGIN_MANIFEST_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return CLIENT_VERSION_FALLBACK
    if isinstance(parsed, dict) and isinstance(parsed.get("version"), str):
        return parsed["version"]
    return CLIENT_VERSION_FALLBACK


CLIENT_VERSION = load_client_version()


def preview_text(text: str, limit: int = 500) -> str:
    if len(text) <= limit:
        return text
    return f"{text[:limit]}..."


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--limit must be an integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("--limit must be >= 1")
    return parsed


def load_repos() -> dict[str, dict[str, Any]]:
    try:
        with REPOS_PATH.open("r", encoding="utf-8") as file:
            repos = json.load(file)
    except FileNotFoundError as exc:
        raise McpError(f"Repository profile file is missing: {REPOS_PATH}") from exc
    except json.JSONDecodeError as exc:
        raise McpError(f"Repository profile file is not valid JSON: {REPOS_PATH}") from exc

    if not isinstance(repos, dict):
        raise McpError(f"Repository profile file must contain a JSON object: {REPOS_PATH}")
    return repos


def resolve_repo(alias_or_name: str) -> str:
    repos = load_repos()
    profile = repos.get(alias_or_name)
    if profile is None:
        for candidate in repos.values():
            if not isinstance(candidate, dict) or not candidate.get("enabled", False):
                continue
            repo_name = candidate.get("repoName")
            if repo_name == alias_or_name:
                return alias_or_name
        known = ", ".join(sorted(repos))
        known_repo_names = ", ".join(
            sorted(
                profile["repoName"]
                for profile in repos.values()
                if isinstance(profile, dict)
                and profile.get("enabled", False)
                and isinstance(profile.get("repoName"), str)
            )
        )
        raise McpError(
            f"Unknown repository '{alias_or_name}'. Known aliases: {known}. "
            f"Known repository names: {known_repo_names}."
        )
    if not isinstance(profile, dict):
        raise McpError(f"Repository profile for '{alias_or_name}' must be a JSON object.")
    if not profile.get("enabled", False):
        raise McpError(
            f"Repository alias '{alias_or_name}' is reserved but not enabled yet "
            f"({profile.get('repoName')})."
        )
    repo_name = profile.get("repoName")
    if not isinstance(repo_name, str) or not repo_name:
        raise McpError(f"Repository alias '{alias_or_name}' is missing a valid repoName.")
    return repo_name


def cache_root() -> Path:
    configured = os.environ.get("DEEPWIKI_MCP_CACHE_DIR")
    if configured:
        return Path(configured).expanduser()
    xdg_cache = os.environ.get("XDG_CACHE_HOME")
    if xdg_cache:
        return Path(xdg_cache).expanduser() / "deepwiki-mcp"
    try:
        return Path.home() / ".cache" / "deepwiki-mcp"
    except RuntimeError:
        return Path(tempfile.gettempdir()) / "deepwiki-mcp"


def repo_cache_dir(repo_name: str) -> Path:
    return cache_root() / repo_name.replace("/", "__")


def contents_cache_path(repo_name: str) -> Path:
    return repo_cache_dir(repo_name) / "wiki-contents.md"


def write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f"{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as tmp_file:
            tmp_file.write(text)
            tmp_path = Path(tmp_file.name)
        tmp_path.replace(path)
    finally:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink()


def parse_json(data: str) -> dict[str, Any]:
    try:
        parsed = json.loads(data)
    except json.JSONDecodeError as exc:
        raise McpError(f"DeepWiki MCP returned non-JSON content: {preview_text(data)}") from exc
    if not isinstance(parsed, dict):
        raise McpError(f"DeepWiki MCP returned an unexpected JSON payload: {preview_text(data)}")
    return parsed


def read_sse_response(response: Any, expected_id: Optional[int]) -> dict[str, Any]:
    data_lines: list[str] = []
    seen_payloads: list[str] = []
    max_seconds = stream_timeout_seconds()
    deadline = time.monotonic() + max_seconds
    timed_out = False

    while True:
        if time.monotonic() > deadline:
            timed_out = True
            break
        try:
            raw_line = response.readline()
        except (TimeoutError, socket.timeout):
            timed_out = True
            break
        if not raw_line:
            break

        line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
        if line.startswith("data:"):
            data_content = line[5:]
            if data_content.startswith(" "):
                data_content = data_content[1:]
            data_lines.append(data_content)
            continue
        if line:
            continue

        if not data_lines:
            continue

        data = "\n".join(data_lines)
        data_lines = []
        seen_payloads.append(data)
        parsed = parse_json(data)
        if expected_id is None or parsed.get("id") == expected_id:
            return parsed

    if data_lines:
        data = "\n".join(data_lines)
        seen_payloads.append(data)
        if not timed_out:
            parsed = parse_json(data)
            if expected_id is None or parsed.get("id") == expected_id:
                return parsed

    preview = "\n".join(seen_payloads[-3:])
    if timed_out:
        raise McpError(
            f"DeepWiki MCP stream timed out waiting for response id {expected_id} "
            f"after {max_seconds:.0f}s: {preview_text(preview)}"
        )
    raise McpError(f"DeepWiki MCP stream ended without response id {expected_id}: {preview_text(preview)}")


class McpClient:
    def __init__(self, endpoint: str, protocol_version: str) -> None:
        parsed_endpoint = urllib.parse.urlparse(endpoint)
        if parsed_endpoint.scheme not in {"http", "https"}:
            scheme = parsed_endpoint.scheme or "<empty>"
            raise McpError(f"Unsupported DeepWiki MCP endpoint scheme: {scheme}")
        self.endpoint = endpoint
        self.protocol_version = protocol_version
        self.session_id: Optional[str] = None
        self.next_id = 1

    def request(self, payload: dict[str, Any], expect_response: bool = True) -> Optional[dict[str, Any]]:
        body = json.dumps(payload).encode("utf-8")
        headers = {
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
            "Mcp-Protocol-Version": self.protocol_version,
            "User-Agent": f"{CLIENT_NAME}/{CLIENT_VERSION}",
        }
        if self.session_id:
            headers["Mcp-Session-Id"] = self.session_id

        req = urllib.request.Request(self.endpoint, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=stream_timeout_seconds()) as response:
                session_id = response.headers.get("Mcp-Session-Id")
                if session_id:
                    self.session_id = session_id
                if not expect_response:
                    return None
                content_type = response.headers.get("Content-Type", "")
                if "text/event-stream" in content_type:
                    parsed = read_sse_response(response, payload.get("id"))
                else:
                    text = response.read().decode("utf-8", errors="replace")
                    if not text.strip():
                        raise McpError("DeepWiki MCP returned an empty response.")
                    parsed = parse_json(text)
        except urllib.error.HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            content_type = exc.headers.get("Content-Type", "unknown")
            raise McpError(
                f"DeepWiki MCP HTTP {exc.code} ({content_type}): {preview_text(details)}"
            ) from exc
        except (TimeoutError, socket.timeout) as exc:
            raise McpError(f"DeepWiki MCP request timed out after {stream_timeout_seconds():.0f}s.") from exc
        except urllib.error.URLError as exc:
            if isinstance(exc.reason, (TimeoutError, socket.timeout)):
                raise McpError(f"DeepWiki MCP request timed out after {stream_timeout_seconds():.0f}s.") from exc
            raise McpError(f"Could not reach DeepWiki MCP endpoint: {exc.reason}") from exc

        if "error" in parsed:
            raise McpError(f"DeepWiki MCP error: {json.dumps(parsed['error'], ensure_ascii=False)}")
        return parsed

    def rpc(self, method: str, params: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"jsonrpc": "2.0", "id": self.next_id, "method": method}
        self.next_id += 1
        if params is not None:
            payload["params"] = params
        result = self.request(payload)
        if result is None:
            raise McpError(f"DeepWiki MCP returned no response for {method}.")
        return result

    def notify(self, method: str, params: Optional[dict[str, Any]] = None) -> None:
        payload: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            payload["params"] = params
        self.request(payload, expect_response=False)

    def initialize(self) -> None:
        self.rpc(
            "initialize",
            {
                "protocolVersion": self.protocol_version,
                "capabilities": {},
                "clientInfo": {"name": CLIENT_NAME, "version": CLIENT_VERSION},
            },
        )
        self.notify("notifications/initialized", {})

    def call_tool(self, name: str, arguments: dict[str, Any]) -> Any:
        response = self.rpc("tools/call", {"name": name, "arguments": arguments})
        return response.get("result")


def extract_text(result: Any) -> str:
    if isinstance(result, dict):
        content = result.get("content")
        if isinstance(content, list):
            chunks: list[str] = []
            for item in content:
                if isinstance(item, dict):
                    text = item.get("text")
                    if isinstance(text, str):
                        chunks.append(text)
                    elif item.get("type") == "json":
                        chunks.append(json.dumps(item, ensure_ascii=False, indent=2))
            if chunks:
                return "\n\n".join(chunks)
        if "structuredContent" in result:
            return json.dumps(result["structuredContent"], ensure_ascii=False, indent=2)
    return json.dumps(result, ensure_ascii=False, indent=2)


def output_tool_result(client: McpClient, tool: str, arguments: dict[str, Any]) -> None:
    client.initialize()
    result = client.call_tool(tool, arguments)
    print(extract_text(result))


def read_wiki_contents(client: McpClient, repo_name: str) -> str:
    client.initialize()
    result = client.call_tool("read_wiki_contents", {"repoName": repo_name})
    return extract_text(result)


def ensure_cached_contents(client: McpClient, repo_name: str, refresh: bool = False) -> tuple[str, Path, str]:
    path = contents_cache_path(repo_name)
    if path.exists() and not refresh:
        try:
            return path.read_text(encoding="utf-8"), path, "reused local cache"
        except (OSError, UnicodeError):
            pass

    text = read_wiki_contents(client, repo_name)
    try:
        write_text_atomic(path, text)
    except (OSError, UnicodeError) as exc:
        return text, path, f"fetched from DeepWiki; cache write skipped ({exc})"
    return text, path, "refreshed from DeepWiki"


def query_terms(query: str) -> list[str]:
    raw_terms = re.findall(r"[\w./:-]+|[\u4e00-\u9fff]+", query.lower())
    terms: list[str] = []
    for term in raw_terms:
        normalized = term.strip("._/:;-")
        if len(normalized) < 2 or normalized in STOPWORDS:
            continue
        if normalized not in terms:
            terms.append(normalized)
    return terms


def build_term_patterns(terms: list[str]) -> list[tuple[re.Pattern[str], int]]:
    patterns: list[tuple[re.Pattern[str], int]] = []
    for term in terms:
        pattern = rf"(?<![a-z0-9_]){re.escape(term)}(?![a-z0-9_])"
        weight = max(1, min(len(term), 12))
        patterns.append((re.compile(pattern), weight))
    return patterns


def score_window(lowered: str, patterns: list[tuple[re.Pattern[str], int]]) -> int:
    score = 0
    for pattern, weight in patterns:
        count = len(pattern.findall(lowered))
        if count:
            score += count * weight
    if "relevant source files" in lowered:
        score -= 40
    if lowered.count("src/main/") > 4 or lowered.count(".java") > 6:
        score -= 60
    return score


def search_cached_context(contents: str, query: str, limit: int) -> list[tuple[int, int, int, str]]:
    terms = query_terms(query)
    if not terms:
        return []
    patterns = build_term_patterns(terms)

    lines = contents.splitlines()
    candidates: list[tuple[int, int, int, str]] = []

    for start in range(0, len(lines), CONTEXT_STRIDE):
        end = min(len(lines), start + CONTEXT_WINDOW_SIZE)
        window = "\n".join(lines[start:end]).strip()
        if not window:
            continue
        score = score_window(window.lower(), patterns)
        if score > 0:
            candidates.append((score, start + 1, end, window))

    candidates.sort(key=lambda item: item[0], reverse=True)
    selected: list[tuple[int, int, int, str]] = []
    selected_ranges: list[tuple[int, int]] = []
    for candidate in candidates:
        _, start, end, _ = candidate
        if any(start <= kept_end and end >= kept_start for kept_start, kept_end in selected_ranges):
            continue
        selected.append(candidate)
        selected_ranges.append((start, end))
        if len(selected) >= limit:
            break
    return selected


def output_context(client: McpClient, repo_name: str, query: str, limit: int, refresh: bool) -> None:
    contents, path, cache_status = ensure_cached_contents(client, repo_name, refresh)
    matches = search_cached_context(contents, query, limit)

    print("# DeepWiki Cached Context")
    print(f"Repository: {repo_name}")
    print(f"Cache file: {path.name}")
    print(f"Cache status: {cache_status}")
    print(f"Query: {query}")
    print()

    if not matches:
        print("No relevant cached DeepWiki wiki snippets were found for this query.")
        print("Fallback: use the `ask` command to request an online DeepWiki answer.")
        return

    for index, (score, start, end, snippet) in enumerate(matches, start=1):
        print(f"## Snippet {index} (score: {score}, lines: {start}-{end})")
        print("```text")
        print(snippet[:4000])
        print("```")
        print()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ask the official DeepWiki MCP server.")
    parser.add_argument(
        "--endpoint",
        default=os.environ.get("DEEPWIKI_MCP_ENDPOINT", DEFAULT_ENDPOINT),
        help=f"DeepWiki MCP endpoint. Defaults to {DEFAULT_ENDPOINT}.",
    )
    parser.add_argument(
        "--protocol-version",
        default=os.environ.get("DEEPWIKI_MCP_PROTOCOL_VERSION", "2025-06-18"),
        help="MCP protocol version to send during initialize.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    ask = subparsers.add_parser("ask", help="Ask a repository question.")
    ask.add_argument("--repo", default="hugegraph", help="Repository alias.")
    ask.add_argument("--question", required=True, help="Question to ask DeepWiki.")

    structure = subparsers.add_parser("structure", help="Read wiki structure.")
    structure.add_argument("--repo", default="hugegraph", help="Repository alias.")

    contents = subparsers.add_parser("contents", help="Read wiki contents.")
    contents.add_argument("--repo", default="hugegraph", help="Repository alias.")
    contents.add_argument("--refresh", action="store_true", help="Refresh the local DeepWiki contents cache.")

    context = subparsers.add_parser("context", help="Search cached DeepWiki wiki contents for a question.")
    context.add_argument("--repo", default="hugegraph", help="Repository alias.")
    context.add_argument("--query", required=True, help="Question or keywords to search in cached wiki contents.")
    context.add_argument("--limit", type=positive_int, default=6, help="Maximum number of snippets to print.")
    context.add_argument("--refresh", action="store_true", help="Refresh the local DeepWiki contents cache before search.")

    tools = subparsers.add_parser("tools", help="List MCP tools for troubleshooting.")
    tools.set_defaults(command="tools")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        client = McpClient(args.endpoint, args.protocol_version)
        if args.command == "ask":
            repo_name = resolve_repo(args.repo)
            output_tool_result(
                client,
                "ask_question",
                {"repoName": repo_name, "question": args.question},
            )
        elif args.command == "structure":
            repo_name = resolve_repo(args.repo)
            output_tool_result(client, "read_wiki_structure", {"repoName": repo_name})
        elif args.command == "contents":
            repo_name = resolve_repo(args.repo)
            contents_text, _, _ = ensure_cached_contents(client, repo_name, args.refresh)
            print(contents_text)
        elif args.command == "context":
            repo_name = resolve_repo(args.repo)
            output_context(client, repo_name, args.query, args.limit, args.refresh)
        elif args.command == "tools":
            client.initialize()
            print(json.dumps(client.rpc("tools/list", {}).get("result"), ensure_ascii=False, indent=2))
        else:
            parser.error(f"Unhandled command {args.command}")
    except McpError as exc:
        print(f"deepwiki_mcp.py: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
