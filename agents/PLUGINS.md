# Sneeze Plugin Guide

Sneeze plugins are normal Python distributions that extend the `sneeze`
namespace.

## Naming

- Distribution: `sneeze-plugin-<username>`
- Import package: `sneeze.<username>`
- Entry point group: `sneeze.plugins`
- Entry point value: `<username> = "sneeze.<username>"`

For username `tpn`, the project is `sneeze-plugin-tpn` and the package is
`sneeze.tpn`.

## Commands

Plugin commands live in `sneeze.<username>.commands` and subclass
`sneeze.command.Command`, normally through
`sneeze.commandinvariant.InvariantAwareCommand`.

If multiple plugins expose the same command name, the CLI prefixes the plugin
username. For example, two plugin classes both resolving to `foo` become
`tpn-foo` and `dave-foo`. Core commands keep their unprefixed names.

## Boundaries

Keep the core package generic: command dispatch, run logs, plugin lifecycle,
and shared utilities. Put user-specific commands, dependencies, data paths, and
domain workflows in plugins.

