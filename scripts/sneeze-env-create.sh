#!/usr/bin/env bash
set -euo pipefail

ENV_NAME="${1:-sneeze314t}"
ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

note() { printf '%s\n' "$*" >&2; }
have() { command -v "$1" >/dev/null 2>&1; }

pick_conda() {
  if have mamba; then
    printf '%s' mamba
    return 0
  fi
  if have conda; then
    printf '%s' conda
    return 0
  fi
  return 1
}

pick_env_yml() {
  if [[ "$1" == *t ]]; then
    printf '%s' "${ROOT_DIR}/environment-ft.yml"
  else
    printf '%s' "${ROOT_DIR}/environment.yml"
  fi
}

run_cmd() {
  note "+ $*"
  "$@"
}

CONDA_BIN="$(pick_conda || true)"
if [[ -z "${CONDA_BIN}" ]]; then
  note "ERROR: neither 'mamba' nor 'conda' found on PATH."
  exit 2
fi

ENV_YML="$(pick_env_yml "${ENV_NAME}")"
note "Using ${CONDA_BIN}; env name: ${ENV_NAME}; env file: ${ENV_YML}"
if ! run_cmd "${CONDA_BIN}" env create -n "${ENV_NAME}" -f "${ENV_YML}" -y; then
  note "WARN: env create failed; trying update for an existing env."
fi
run_cmd "${ROOT_DIR}/scripts/sneeze-env-update.sh" "${ENV_NAME}"
