# Roborev Playbook For `sneeze`

Use the explicit matrix gate for the current commit or branch.

Latest commit:

```bash
scripts/roborev-matrix-review.sh HEAD
```

Whole branch:

```bash
scripts/roborev-matrix-review.sh
```

Defaults:

- reviewers: `codex`, `claude-code`, `gemini`
- synthesizer: `claude-code`
- reasoning: `thorough`
- minimum severity: `medium`
- output artifact: `.roborev/last-review.md`

Useful overrides:

```bash
ROBOREV_AGENTS=claude-code scripts/roborev-matrix-review.sh HEAD
ROBOREV_GATE_TIMEOUT=10m scripts/roborev-matrix-review.sh HEAD
ROBOREV_MIN_SEVERITY=low scripts/roborev-matrix-review.sh HEAD
```

