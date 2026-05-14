from sneeze.config import Config


def test_config_class_properties_work_on_supported_python():
    assert Config.namespace == "sneeze"
    assert Config.conf_dir.endswith("/conf")
    assert Config.data_dir.endswith("/data")


def test_data_dir_property_has_no_filesystem_side_effect(tmp_path):
    class TempConfig(Config):
        @classmethod
        def _resolve_dir(cls, name):
            return str(tmp_path / name)

    assert TempConfig.data_dir == str(tmp_path / "data")
    assert not (tmp_path / "data").exists()
    assert TempConfig.ensure_data_dir() == str(tmp_path / "data")
    assert (tmp_path / "data").is_dir()


def test_default_runtime_paths_are_user_scoped():
    from sneeze import command, config

    assert "/site-packages/" not in command.DEFAULT_LOG_DIR
    assert "/site-packages/" not in config.SNEEZE_RUN_DIR
    assert config.SNEEZE_RUN_DIR.endswith("/sneeze/run")
    assert config.SNEEZE_CONF_DIR.endswith("/sneeze")
    assert config.SNEEZE_DATA_DIR.endswith("/sneeze")
