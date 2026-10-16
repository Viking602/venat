#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

BASELINE_FILE=".sentrux/business-words.baseline"
required_scopes=(message provider tool skill agent)

if [[ ! -f "$BASELINE_FILE" ]]; then
  echo "FAIL: missing baseline file: $BASELINE_FILE" >&2
  exit 1
fi
for scope in "${required_scopes[@]}"; do
  if [[ ! -d "$scope" ]]; then
    echo "FAIL: required scope $scope is missing" >&2
    exit 1
  fi
  if ! find "$scope" -maxdepth 1 -type f -name '*.go' ! -name '*_test.go' -print -quit | grep -q .; then
    echo "FAIL: required scope $scope has no production Go files" >&2
    exit 1
  fi
done

python3 - "$BASELINE_FILE" "${required_scopes[@]}" <<'PY'
import pathlib
import re
import sys

baseline_text = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8").strip()
if not baseline_text.isdigit():
    raise SystemExit(f"FAIL: invalid business-word baseline {baseline_text!r}")
baseline = int(baseline_text)
scopes = sys.argv[2:]
pattern = re.compile(
    r"Synthesis|ReviewResult|ActionResult|TaskTypeAction|TaskTypeReview|"
    r"TaskTypeSynthesis|\bHazard\b|\bIncident\b|\bTicket\b|\bCustomer\b|agent_review"
)

offenses = []
for scope in scopes:
    for path in sorted(pathlib.Path(scope).rglob("*.go")):
        if path.name.endswith("_test.go"):
            continue
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            for match in pattern.finditer(line):
                offenses.append((str(path), line_number, match.group(0), line.strip()))

count = len(offenses)
print(f"framework business-word count: {count} (baseline: {baseline})")
if count > baseline:
    for path, line_number, word, line in offenses:
        print(f"{path}:{line_number}: {word}: {line}", file=sys.stderr)
    print(f"FAIL: {count - baseline} new business-word occurrence(s) leaked into SDK code.", file=sys.stderr)
    raise SystemExit(1)
if count < baseline:
    print(f"tip: count dropped below baseline ({count} < {baseline}); lock the lower baseline")
print("OK: framework vocabulary boundary preserved.")
PY
