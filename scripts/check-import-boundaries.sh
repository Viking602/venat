#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

module="$(go list -m -f '{{.Path}}')"
required_scopes=(message provider tool skill agent orchestration durable)
violations=0

is_within() {
  local imported="$1"
  local prefix="$2"
  [[ "$imported" == "$prefix" || "$imported" == "$prefix/"* ]]
}

allowed_for_scope() {
  local scope="$1"
  local imported="$2"
  local allowed

  case "$scope" in
    message) allowed=(message) ;;
    provider) allowed=(provider message) ;;
    tool) allowed=(tool message) ;;
    skill) allowed=(skill) ;;
    agent) allowed=(agent message provider tool skill) ;;
    orchestration) allowed=(orchestration agent message provider tool) ;;
    durable) allowed=(durable agent message provider tool) ;;
    *) return 1 ;;
  esac

  for allowed in "${allowed[@]}"; do
    if is_within "$imported" "$module/$allowed"; then
      return 0
    fi
  done
  return 1
}

for scope in "${required_scopes[@]}"; do
  if [[ ! -d "$scope" ]]; then
    printf 'FAIL [required-scope]: %s is missing\n' "$scope" >&2
    violations=$((violations + 1))
    continue
  fi

  production_count="$(go list -f '{{len .GoFiles}}' "./$scope")"
  if [[ "$production_count" -eq 0 ]]; then
    printf 'FAIL [required-scope]: %s has no production Go files\n' "$scope" >&2
    violations=$((violations + 1))
    continue
  fi

  while IFS=$'\t' read -r kind package imported; do
    [[ -z "${package:-}" || -z "${imported:-}" ]] && continue
    if ! is_within "$imported" "$module"; then
      continue
    fi
    if allowed_for_scope "$scope" "$imported"; then
      continue
    fi
    printf 'FAIL [%s-imports]: %s imports %s (%s)\n' "$scope" "$package" "$imported" "$kind" >&2
    violations=$((violations + 1))
  done < <(
    go list -f '{{$p := .ImportPath}}{{range .Imports}}{{printf "prod\t%s\t%s\n" $p .}}{{end}}{{range .TestImports}}{{printf "test\t%s\t%s\n" $p .}}{{end}}{{range .XTestImports}}{{printf "xtest\t%s\t%s\n" $p .}}{{end}}' "./$scope/..."
  )
done

if ((violations > 0)); then
  printf '\nFAIL: %d import boundary violation(s).\n' "$violations" >&2
  printf 'See docs/architecture-boundaries.md.\n' >&2
  exit 1
fi

echo "OK: import boundaries preserved."
