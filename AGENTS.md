# Repository Guidelines

## Project Structure

- `src/sneeze/` contains the core Python package and `sne` CLI.
- `tests/` contains pytest tests for the core runner and plugin behavior.
- `agents/PLUGINS.md` defines the plugin contract for generated plugins.
- `scripts/` contains local environment and Roborev helper scripts.
- `COMMAND-MAP.md` tracks the bare core command map. Plugin commands are
  environment-dependent and are not tracked here.

## Development Commands

- `python -m pip install -e ".[dev]"` installs the package for development.
- `python -m pytest` runs tests.
- `python -m ruff check .` runs lint checks.
- `python -m black --check .` verifies formatting.
- `sne ...` runs the CLI.
- `sne init-plugin tpn` bootstraps `~/src/sneeze-plugin-tpn`.
- `sne install-plugin tpn` installs the local TPN plugin editable when present.

## Style

- Python code uses 4 spaces and a maximum line length of 78.
- Black and Ruff are authoritative; do not reformat unrelated files.
- Keep core `sneeze` generic. User-specific functionality belongs in plugins.
- Prefer small command classes and helpers that can be tested without shelling
  out.

## Roborev Workflow

This repo uses an explicit local Roborev matrix gate rather than a post-commit
hook. After each commit, run:

```bash
scripts/roborev-matrix-review.sh HEAD
```

Before handing off a branch, run:

```bash
scripts/roborev-matrix-review.sh
```

The default matrix is `codex`, `claude-code`, and `gemini`, synthesized by
`claude-code`. The stable review artifact is `.roborev/last-review.md`.

Current machine prerequisites must pass before the gate is authoritative:

```bash
roborev status
roborev check-agents --agent codex --timeout 30
roborev check-agents --agent claude-code --timeout 30
roborev check-agents --agent gemini --timeout 30
```

