#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

removed_dirs=(
  api worker policy session transport packs coding eval cmd contract internal
  multiagent hook stream _examples workflow blackboard orchestration durable
)
violations=0

for path in "${removed_dirs[@]}"; do
  if [[ -e "$path" ]]; then
    echo "FAIL [removed-directory]: $path exists" >&2
    violations=$((violations + 1))
  fi
done

if find . -maxdepth 1 -type f -name '*.go' -print -quit | grep -q .; then
  echo "FAIL [root-package]: root Go source exists; applications must use direct package imports" >&2
  violations=$((violations + 1))
fi

module="$(go list -m -f '{{.Path}}')"
# Capture first: a process substitution would discard go list's exit status
# and let an unloadable package tree pass with a partial scan.
packages="$(go list -f '{{.ImportPath}}' ./...)"
while IFS= read -r package; do
  case "$package" in
    "$module"/message|"$module"/message/*|"$module"/provider|"$module"/provider/*|"$module"/tool|"$module"/tool/*|"$module"/skill|"$module"/skill/*|"$module"/agent|"$module"/agent/*|"$module"/examples/*|"$module"/scripts/publicany)
      ;;
    *)
      echo "FAIL [package-graph]: unexpected production package $package" >&2
      violations=$((violations + 1))
      ;;
  esac
done <<<"$packages"

if ! python3 <<'PY'
import pathlib
import re
import sys

root = pathlib.Path(".")
skip_dirs = {".git"}
legacy_import = re.compile(
    r'"github\.com/Viking602/venat(?:"|/(?:api|worker|policy|session|transport|packs|coding|eval|contract|multiagent|hook|stream|internal|orchestration|durable)(?:/[^"\s]*)?")'
)
legacy_symbol = re.compile(
    r"\b(?:StoreProvider|UnitOfWork|Runner|TurnControl|ContextTransition|AsTool)\b|"
    r"\bapi\.Task\b|\bagent\.Harness\b|\bsession\.Storage\b|"
    r"\bStepDecisionHandoff\b"
)

def production_go_files():
    for path in root.rglob("*.go"):
        if any(part in skip_dirs for part in path.parts) or path.name.endswith("_test.go"):
            continue
        yield path

current_docs = [
    pathlib.Path("AGENTS.md"),
    pathlib.Path("README.md"),
    pathlib.Path("CONTRIBUTING.md"),
    pathlib.Path("RELEASING.md"),
]
for path in pathlib.Path("docs").glob("*.md"):
    if path.name not in {"migration.md", "release-v0.4.0.md"}:
        current_docs.append(path)
for directory in (pathlib.Path("docs/plans"),):
    if directory.exists():
        current_docs.extend(directory.rglob("*.md"))

violations = []
for path in production_go_files():
    text = path.read_text(encoding="utf-8")
    for pattern, label in ((legacy_import, "legacy import"), (legacy_symbol, "legacy symbol")):
        for match in pattern.finditer(text):
            line = text.count("\n", 0, match.start()) + 1
            violations.append((str(path), line, label, match.group(0)))

for path in sorted(set(current_docs)):
    if not path.exists():
        violations.append((str(path), 0, "missing current document", ""))
        continue
    text = path.read_text(encoding="utf-8")
    for match in legacy_import.finditer(text):
        line = text.count("\n", 0, match.start()) + 1
        violations.append((str(path), line, "legacy import in current docs", match.group(0)))
    for match in legacy_symbol.finditer(text):
        line = text.count("\n", 0, match.start()) + 1
        violations.append((str(path), line, "legacy symbol in current docs", match.group(0)))

if violations:
    for path, line, label, value in violations:
        print(f"FAIL [{label}]: {path}:{line}: {value}", file=sys.stderr)
    raise SystemExit(1)
print("OK: removed packages and symbols are absent from production code and current docs.")
PY
then
  violations=$((violations + 1))
fi

if ((violations > 0)); then
  echo "FAIL: $violations removed-surface or package-graph gate(s) failed." >&2
  exit 1
fi
