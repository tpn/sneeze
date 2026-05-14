from sneeze.config import Config


def test_config_class_properties_work_on_supported_python():
    assert Config.namespace == "sneeze"
    assert Config.conf_dir.endswith("/conf")
    assert Config.data_dir.endswith("/data")
