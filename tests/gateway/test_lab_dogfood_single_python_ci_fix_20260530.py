"""Minimal lab dogfood smoke test for the gateway API server adapter."""

from gateway.config import PlatformConfig
from gateway.platforms.api_server import APIServerAdapter


def test_lab_dogfood_api_server_adapter_constructs_from_explicit_config():
    config = PlatformConfig(
        enabled=True,
        extra={
            "host": "127.0.0.1",
            "port": "8642",
            "key": "",
            "cors_origins": "https://lab.example, https://ci.example",
            "model_name": "hermes-lab-smoke",
        },
    )

    adapter = APIServerAdapter(config)

    assert adapter._host == "127.0.0.1"
    assert adapter._port == 8642
    assert adapter._api_key == ""
    assert adapter._cors_origins == ("https://lab.example", "https://ci.example")
    assert adapter._model_name == "hermes-lab-smoke"
    assert adapter._run_streams == {}
    assert adapter._run_statuses == {}
