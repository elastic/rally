import json
import subprocess

import pytest

from .conftest import ServerlessProjectConfig

TRACKS = ["nyc_taxis", "pmc", "elastic/logs"]


def client_options(username, password):
    return {
        "default": {
            "verify_certs": False,
            "use_ssl": True,
            "basic_auth_user": username,
            "timeout": 240,
            "basic_auth_password": password,
        }
    }


@pytest.mark.parametrize("track", TRACKS)
def test_serverless(track, tmp_path, serverless_project_config: ServerlessProjectConfig):
    options_path = tmp_path / "options.json"
    with options_path.open("w") as f:
        json.dump(client_options(serverless_project_config.username, serverless_project_config.password), fp=f)

    subprocess.run(
        [
            "esrally",
            "race",
            f"--track={track}",
            "--track-params=number_of_shards:1,number_of_replicas:1",
            # TODO: should we run a full test instead?
            "--test-mode",
            f"--target-hosts={serverless_project_config.target_host}",
            "--pipeline=benchmark-only",
            f"--client-options={str(options_path)}",
            "--user-tags='intention:rally-it-serverless'",
            "--on-error=abort",
        ],
        check=True,
    )
