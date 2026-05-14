import pytest


@pytest.fixture(autouse=True)
def isolate_sneeze_log_dir(tmp_path, monkeypatch):
    from sneeze import command

    monkeypatch.setattr(command, "DEFAULT_LOG_DIR", str(tmp_path / "logs"))
