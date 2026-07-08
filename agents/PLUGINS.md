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

## Slackbot User Secrets

Plugins that define Slackbot profiles can extend the per-user secret fields
without adding service-specific names to Sneeze core. Pass extra
`SlackbotUserConfigSecretField` entries when constructing the profile:

```python
from sneeze.slackbot import SlackbotProfile, SlackbotUserConfigSecretField

PROFILE = SlackbotProfile(
    app_slug="example",
    env_prefix="EXAMPLE",
    default_bot_name="example",
    default_command_name="/example",
    default_runtime_root="~/.local/state/example/slackbot",
    default_codex_workdir="~/src/example",
    default_system_prompt="# Example\n",
    user_config_secret_fields=(
        SlackbotUserConfigSecretField(
            service_name="vendor",
            label="Vendor token",
            env_names=("VENDOR_TOKEN",),
            required=False,
        ),
    ),
)
```

The stored token lives under the requesting Slack user's `secrets/` directory
and is injected only into that user's Codex child process. Secret fields are
optional by default; set `required=True` only when every user-scoped Codex run
for that profile must fail fast until the user stores that token. Slack modals
show secret status only; they must not collect token material.

Slackbot Codex runs scrub ambient secret-looking environment variables as a
best-effort safety boundary. The scrubber treats common credential words such
as `AUTH`, `KEY`, `PRIVATE`, `SECRET`, `SIGNING`, and `TOKEN` as sensitive.
User-scoped runs do not preserve ambient secret-named variables; credentials
needed by a requesting user must be represented as `user_config_secret_fields`,
or loaded from non-env file-based config such as the tool's home directory.
Names declared by `user_config_secret_fields` and `scrub_env_names` are always
scrubbed from the ambient child environment.

Plugins that deliberately add a secret-named variable to child-process
passthrough can add that variable to
`SlackbotProfile.child_env_scrub_allowlist`. Do not use the allowlist for
variables that are also user-scoped secret fields. User-scoped channel and DM
Codex runs honor this allowlist for ambient tool credentials, while names
declared by `user_config_secret_fields` are still replaced by per-user values.

Migration note: channel and DM Codex runs are user-scoped. Existing ambient
credentials such as process-wide `*_TOKEN` variables will be scrubbed from
those runs; move them to `user_config_secret_fields` or file-based tool config.
Conversation continuity for existing channel sessions can reset once per-user
conversation keys are introduced.

Scheduled Codex jobs are system-scoped. Ambient secret-named variables are
scrubbed from scheduled runs too unless the plugin deliberately includes that
variable in `child_env_scrub_allowlist`; prefer file-based tool config whenever
possible.

## Public Channel Collaboration

Channel trust admits public app mentions, but it does not make every channel
participant an operator. A participant who is not allowed by user ID receives
an isolated, non-resumable app-server turn with an explicit empty environment
list. Local, web, browser, app, plugin, computer, and shell tools are disabled;
the turn has no user secrets, service adapters, tmux controls, slash commands,
or config access.

Profiles can define a narrower explicit admin list. When they do not,
`load_config()` preserves the legacy fallback from allowed users to admins.

Profiles can populate `known_collaborator_user_ids` to label familiar people in
the contextual prompt. This is relationship context only; it grants no added
authority. `codex_progress_update_seconds` enables periodic edits of the
working placeholder, and `contextual_codex_timeout_seconds` bounds contextual
turns. Only one contextual turn runs at a time, preserving worker capacity for
operator requests; admission happens before Slack context is fetched.
Nonoperator public socket events enter a separately bounded contextual
dispatch path before the primary worker queue, and excess events are dropped
at that boundary. Public context keeps the thread root plus an adaptive recent
window, bounded to six
logical Slack reads, a shared 12-attempt/15-second API budget, 50 messages, and
a 24,000-character transcript. Contextual transcripts use stable IDs without
profile lookups, and contextual replies neutralize Slack mention/control tokens
before posting. Operator transcript rendering caches failed profile lookups and
caps new identity lookups at 20 per context fetch.
`contextual_codex_model` must name a model whose exposed tool surface is
validated for the model-only path; contextual turns fail closed when it is not
set or not present in the core validated-model allowlist. Contextual work uses
a dedicated single-worker executor, so the primary executor remains exactly at
the configured operator `worker_count` while public work runs separately.
