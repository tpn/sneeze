#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

input_ref="${1:-}"
if [[ -z "${input_ref}" ]]; then
  default_branch="$(
    git symbolic-ref --quiet --short refs/remotes/origin/HEAD 2>/dev/null ||
      true
  )"
  default_branch="${default_branch#origin/}"
  if [[ -n "${default_branch}" ]] &&
    git rev-parse --verify --quiet "origin/${default_branch}" >/dev/null
  then
    base_ref="origin/${default_branch}"
  elif git rev-parse --verify --quiet origin/main >/dev/null; then
    base_ref="origin/main"
  elif git rev-parse --verify --quiet main >/dev/null; then
    base_ref="main"
  else
    echo "Could not resolve a default branch ref." >&2
    echo "Pass an explicit ref such as HEAD." >&2
    exit 1
  fi
  merge_base="$(git merge-base "${base_ref}" HEAD)"
  ref="${merge_base}..HEAD"
elif [[ "${input_ref}" == *..* ]]; then
  ref="${input_ref}"
elif git rev-parse --verify --quiet "${input_ref}^" >/dev/null; then
  ref="${input_ref}^..${input_ref}"
else
  ref="${input_ref}"
fi

output_path="${ROBOREV_OUTPUT_PATH:-${repo_root}/.roborev/last-review.md}"
gate_timeout="${ROBOREV_GATE_TIMEOUT:-}"
agents_csv="${ROBOREV_AGENTS:-codex,claude-code,gemini}"
review_types_csv="${ROBOREV_REVIEW_TYPES:-default}"
reasoning="${ROBOREV_REASONING:-thorough}"
min_severity="${ROBOREV_MIN_SEVERITY:-medium}"
synthesis_agent="${ROBOREV_SYNTHESIS_AGENT:-claude-code}"

mkdir -p "$(dirname "${output_path}")"

run_with_optional_timeout() {
  if [[ -n "${gate_timeout}" ]] && command -v timeout >/dev/null 2>&1; then
    timeout --foreground "${gate_timeout}" "$@"
  else
    "$@"
  fi
}

{
  printf '# Roborev Matrix Review\n\n'
  printf -- '- Ref: `%s`\n' "${ref}"
  printf -- '- Started: `%s`\n' "$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
  printf -- '- Reviewers: `%s`\n' "${agents_csv}"
  printf -- '- Review types: `%s`\n' "${review_types_csv}"
  printf -- '- Synthesis agent: `%s`\n' "${synthesis_agent}"
  printf -- '- Reasoning: `%s`\n' "${reasoning}"
  printf -- '- Minimum severity: `%s`\n\n' "${min_severity}"
  printf '## Review Output\n\n'
} >"${output_path}"

run_with_optional_timeout roborev ci review \
  --ref "${ref}" \
  --agent "${agents_csv}" \
  --review-types "${review_types_csv}" \
  --synthesis-agent "${synthesis_agent}" \
  --reasoning "${reasoning}" \
  --min-severity "${min_severity}" |
  tee -a "${output_path}"

