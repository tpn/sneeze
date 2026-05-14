from sneeze.plugin import (
    github_shorthand_to_url,
    plugin_dist_name,
    plugin_package_name,
    resolve_plugin_install_target,
    scaffold_plugin,
)


def test_plugin_names():
    assert plugin_dist_name("tpn") == "sneeze-plugin-tpn"
    assert plugin_package_name("tpn") == "sneeze.tpn"


def test_github_shorthand_to_url():
    assert github_shorthand_to_url("gh:tpn/sneeze-plugin-tpn") == (
        "git+https://github.com/tpn/sneeze-plugin-tpn.git"
    )


def test_install_target_prefers_local_sibling(tmp_path):
    local = tmp_path / "sneeze-plugin-tpn"
    local.mkdir()

    target = resolve_plugin_install_target("tpn", src_dir=str(tmp_path))

    assert target == str(local)


def test_install_target_falls_back_to_github():
    target = resolve_plugin_install_target("tpn", src_dir="/does/not/exist")

    assert target == "git+https://github.com/tpn/sneeze-plugin-tpn.git"


def test_scaffold_plugin_writes_expected_files(tmp_path):
    path = scaffold_plugin(
        "tpn",
        output_dir=tmp_path / "sneeze-plugin-tpn",
        init_git=False,
    )

    assert (path / "README.md").exists()
    assert (path / "AGENTS.md").exists()
    assert (path / "pyproject.toml").exists()
    assert (path / "src/sneeze/tpn/commands.py").exists()
    assert "../sneeze/agents/PLUGINS.md" in (path / "AGENTS.md").read_text(
        encoding="utf-8"
    )
