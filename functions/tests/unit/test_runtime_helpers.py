import re

import pytest

import set_environment
import var_funcs


def test_load_config_reads_yaml_file(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text("environment:\n  value: test\n")

    assert set_environment.load_config(str(config_path)) == {
        "environment": {"value": "test"}
    }


def test_get_environment_config_returns_requested_environment():
    assert set_environment.get_environment_config(
        {"dev": {"value": 1}}, "dev"
    ) == {"value": 1}


def test_get_environment_config_raises_for_unknown_environment():
    with pytest.raises(ValueError, match="Environment 'missing' not found"):
        set_environment.get_environment_config({}, "missing")


def test_current_date_and_time_use_expected_formats():
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", var_funcs.current_date())
    assert re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}\+0000",
        var_funcs.current_time(),
    )
