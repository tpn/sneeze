# Sneeze

Sneeze is a small plugin-oriented command framework.

The installed console script is `sne`. The core package stays intentionally
small: it provides the command runner, run logging, `run-history`, and plugin
bootstrap/install/remove commands. User-specific commands should live in
plugins such as `sneeze-plugin-tpn`, imported as `sneeze.tpn`.

## Development

Create the conda environment:

```bash
./scripts/sneeze-env-create.sh
```

Install in an existing environment:

```bash
python -m pip install -e ".[dev]"
```

Run validation:

```bash
python -m pytest
python -m ruff check .
python -m black --check .
```

## Plugins

Create a plugin scaffold:

```bash
sne init-plugin tpn
```

Install a plugin:

```bash
sne install-plugin tpn
```

For a bare username, `install-plugin` first checks for a local sibling repo at
`~/src/sneeze-plugin-<username>` and installs it editable when present. If the
local repo is missing, it falls back to
`gh:<username>/sneeze-plugin-<username>`.

Multiple plugins can be installed. If two plugins expose the same command name,
Sneeze prefixes the plugin username, such as `tpn-foo` and `dave-foo`. Core
commands keep the unprefixed name.

