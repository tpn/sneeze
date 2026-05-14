from sneeze.config import Config


def test_config_class_properties_work_on_supported_python():
    assert Config.namespace == "sneeze"
    assert Config.conf_dir.endswith("/conf")
    assert Config.data_dir.endswith("/data")


def test_default_runtime_paths_are_user_scoped():
    from sneeze import command, config

    assert "/site-packages/" not in command.DEFAULT_LOG_DIR
    assert "/site-packages/" not in config.SNEEZE_RUN_DIR
    assert config.SNEEZE_RUN_DIR.endswith("/sneeze/run")
    assert config.SNEEZE_CONF_DIR.endswith("/sneeze")
    assert config.SNEEZE_DATA_DIR.endswith("/sneeze")
