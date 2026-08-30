#!/usr/bin/env python3
"""Assert a file makes NO environment access, by inspecting its SYNTAX TREE.

Exists because the grep version of this check matched the word "environment" inside the
target's own docstring — a guard firing on its own explanatory prose, reporting a defect
that did not exist. Prose cannot reach an AST, so the AST is what is inspected.

Exit 0 = no environment access. Exit 1 = access found (printed). Exit 2 = unparseable.
"""
import ast
import sys

def main(path: str) -> int:
    try:
        tree = ast.parse(open(path, encoding="utf-8").read())
    except (OSError, SyntaxError) as exc:
        print(f"cannot parse {path}: {exc}")
        return 2
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] == "os":
                    found.append(f"line {node.lineno}: import {alias.name}")
        elif isinstance(node, ast.ImportFrom):
            if (node.module or "").split(".")[0] == "os":
                found.append(f"line {node.lineno}: from {node.module} import ...")
        elif isinstance(node, ast.Attribute) and node.attr in ("environ", "getenv"):
            found.append(f"line {node.lineno}: .{node.attr}")
        elif isinstance(node, ast.Name) and node.id in ("environ", "getenv"):
            found.append(f"line {node.lineno}: {node.id}")
    if found:
        print("; ".join(found))
        return 1
    return 0

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: ws0_assert_no_env_access.py <file.py>")
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
