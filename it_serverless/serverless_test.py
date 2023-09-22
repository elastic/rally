import json
import subprocess

import pytest

from .conftest import ServerlessProjectConfig

TRACKS = ["nyc_taxis", "pmc", "elastic/logs"]


def client_options(client_auth):
    return {
        "default": {
            "verify_certs": False,
            "use_ssl": True,
            "timeout": 240,
            **client_auth,
        }
    }


@pytest.mark.parametrize("operator", [True, False])
@pytest.mark.parametrize("track", TRACKS)
def test_serverless(operator, track, tmp_path, serverless_project_config: ServerlessProjectConfig):
    if operator:
        client_auth = {
            "basic_auth_user": serverless_project_config.username,
            "basic_auth_password": serverless_project_config.password,
        }
    else:
        client_auth = {"api_key": serverless_project_config.api_key}

    options_path = tmp_path / "options.json"
    with options_path.open("w") as f:
        json.dump(client_options(client_auth), fp=f)

    try:
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
    except subprocess.CalledProcessError:
        if not operator:
            pytest.xfail("Rally does not support serverless in public mode yet.")
        else:
            raise
