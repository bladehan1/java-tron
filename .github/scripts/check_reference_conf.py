#!/usr/bin/env python3
"""Validate java-tron reference.conf key names and hierarchy depth.

Rules enforced:
  1. Every dot-separated segment of every key path must match ^[a-z][a-zA-Z0-9]*$
     (lowerCamelCase: starts lowercase, letters/digits only).
  2. Total path depth must be <= MAX_DEPTH.
  3. ALLOWLIST entries are exempt from the format rule (legacy keys that ship in
     user configs; renaming would break compatibility).

Exit code: 0 if clean, 1 if any violation remains after allowlist filtering.
Findings are printed to stdout; the per-line failure summary goes to stderr.

CI integration: this script is invoked by the `Validate reference.conf key
names and depth` step of the `checkstyle` job in `.github/workflows/pr-check.yml`.
The non-zero exit on violations (`sys.exit(1)` below) is what makes that step
fail — there is intentionally NO extra `exit 1` in the workflow shell wrapper.
A single GHA `::error` workflow command is also emitted unconditionally (not
gated on the GITHUB_ACTIONS env var) so local runs produce the same output as
CI, simplifying dev iteration; the leading `::` is harmless noise locally.
"""
import re
import sys
from pathlib import Path

MAX_DEPTH = 5
KEY_REGEX = re.compile(r'^[a-z][a-zA-Z0-9]*$')
ALLOWLIST = {
    "node.http.PBFTEnable",
    "node.http.PBFTPort",
    "node.rpc.PBFTEnable",
    "node.rpc.PBFTPort",
}


def extract_keys(src: str):
    """Yield (line_number, full_dotted_path) for every assignment/object key.

    The scanner:
      - tracks brace/bracket frames so array element scopes don't append to the
        path prefix;
      - skips "..."-quoted strings (with \\ escapes) — critical because values
        like `url = "http://x"` contain `//` that must not be read as a comment;
      - strips `#` and `//` line comments only outside string literals;
      - recognises a key as an identifier (possibly dotted) followed by '=', ':',
        or '{'.
    """
    ident_re = re.compile(r'[A-Za-z_][A-Za-z0-9_\-]*(?:\.[A-Za-z_][A-Za-z0-9_\-]*)*')
    frames = [{"kind": "obj", "prefix": []}]
    line = 1
    pos = 0
    n = len(src)
    while pos < n:
        c = src[pos]
        if c == '\n':
            line += 1
            pos += 1
            continue
        if c in ' \t\r,':
            pos += 1
            continue
        if c == '#':
            while pos < n and src[pos] != '\n':
                pos += 1
            continue
        if c == '/' and pos + 1 < n and src[pos + 1] == '/':
            while pos < n and src[pos] != '\n':
                pos += 1
            continue
        if c == '{':
            # anonymous object (e.g. array element); inherits current prefix
            # for arrays, prefix should NOT extend; for nested objects without
            # a key, just preserve prefix unchanged.
            frames.append({"kind": "obj", "prefix": list(frames[-1]["prefix"])})
            pos += 1
            continue
        if c == '[':
            frames.append({"kind": "arr", "prefix": list(frames[-1]["prefix"])})
            pos += 1
            continue
        if c == '}' or c == ']':
            if len(frames) > 1:
                frames.pop()
            pos += 1
            continue
        if c == '"':
            pos += 1
            while pos < n:
                if src[pos] == '\\':
                    if pos + 1 < n and src[pos + 1] == '\n':
                        line += 1
                    pos += 2
                    continue
                if src[pos] == '"':
                    pos += 1
                    break
                if src[pos] == '\n':
                    line += 1
                pos += 1
            continue
        m = ident_re.match(src, pos)
        if m:
            ident = m.group(0)
            end = m.end()
            p2 = end
            while p2 < n and src[p2] in ' \t':
                p2 += 1
            if p2 < n and src[p2] in '=:{':
                parts = ident.split('.')
                full_parts = list(frames[-1]["prefix"]) + parts
                yield (line, '.'.join(full_parts), parts)
                if src[p2] == '{':
                    frames.append({"kind": "obj", "prefix": full_parts})
                    pos = p2 + 1
                else:
                    pos = p2 + 1
                continue
            pos = end
            continue
        pos += 1


def main(argv):
    if len(argv) != 2:
        print(f"usage: {argv[0]} <path/to/reference.conf>", file=sys.stderr)
        return 2
    path = Path(argv[1])
    if not path.is_file():
        print(f"error: file not found: {path}", file=sys.stderr)
        return 2

    src = path.read_text(encoding="utf-8")
    format_violations = []
    depth_violations = []
    seen_paths = set()

    for line, full_path, parts in extract_keys(src):
        if full_path in seen_paths:
            continue
        seen_paths.add(full_path)

        if full_path not in ALLOWLIST:
            for seg in parts:
                if not KEY_REGEX.match(seg):
                    format_violations.append((line, full_path, seg))
                    break

        depth = len(full_path.split('.'))
        if depth > MAX_DEPTH:
            depth_violations.append((line, full_path, depth))

    format_violations.sort(key=lambda v: (v[1], v[0]))
    depth_violations.sort(key=lambda v: (v[1], v[0]))

    lines_out = []
    if format_violations:
        lines_out.append(
            f"Format violations ({len(format_violations)}) — "
            f"each segment must match {KEY_REGEX.pattern}:"
        )
        for line, full_path, seg in format_violations:
            lines_out.append(f"  format: {full_path}   (segment: '{seg}', line {line})")
    if depth_violations:
        if lines_out:
            lines_out.append("")
        lines_out.append(
            f"Depth violations ({len(depth_violations)}) — max depth is {MAX_DEPTH}:"
        )
        for line, full_path, depth in depth_violations:
            lines_out.append(
                f"  depth: {full_path}   (depth={depth}, line {line}, max={MAX_DEPTH})"
            )

    if format_violations or depth_violations:
        print("\n".join(lines_out))
        print()
        # Emit ONE consolidated GHA workflow annotation with the file path
        # attached so the PR Checks panel and Files Changed view link to
        # reference.conf. All offending entries are packed into the annotation
        # body via %0A (GHA's newline escape) so the entries are visible in the
        # annotation summary, not just in the job log.
        entries = []
        for line, full_path, seg in format_violations:
            entries.append(
                f"format: {full_path} (segment '{seg}', line {line})"
            )
        for line, full_path, depth in depth_violations:
            entries.append(
                f"depth: {full_path} (depth={depth}, line {line}, max={MAX_DEPTH})"
            )
        body = (
            f"reference.conf has {len(format_violations)} format + "
            f"{len(depth_violations)} depth violation(s):%0A"
            + "%0A".join(entries)
        )
        print(f"::error file={path},title=reference.conf::{body}")
        print(
            f"FAIL: {len(format_violations)} format + {len(depth_violations)} depth "
            f"violation(s) in {path}",
            file=sys.stderr,
        )
        # The CI step relies on this non-zero exit to fail — see module docstring.
        return 1

    print(f"OK: {path} — {len(seen_paths)} keys, all lowerCamelCase, depth <= {MAX_DEPTH}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
