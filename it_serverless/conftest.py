import collections
import contextlib
import json
import os
import subprocess
import time
import urllib.parse

import pytest
import requests
from elasticsearch import Elasticsearch

BASE_URL = os.environ["RALLY_IT_SERVERLESS_BASE_URL"]
API_KEY = os.environ["RALLY_IT_SERVERLESS_API_KEY"]
GET_CREDENTIALS_ENDPOINT = os.environ["RALLY_IT_SERVERLESS_GET_CREDENTIALS_ENDPOINT"]

TRACKS = ["nyc_taxis", "pmc", "elastic/logs"]


ServerlessProjectConfig = collections.namedtuple(
    "ServerlessProjectConfig",
    ["target_host", "username", "password", "api_key"],
)


def serverless_api(method, endpoint, json=None):
    resp = requests.request(
        method,
        BASE_URL + endpoint,
        headers={
            "Authorization": f"ApiKey {API_KEY}",
            "Content-Type": "application/json",
        },
        json=json,
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


@pytest.fixture(scope="session")
def serverless_project():
    print("\nCreating project")
    created_project = serverless_api(
        "POST",
        "/api/v1/serverless/projects/elasticsearch",
        json={
            "name": "rally-it-serverless",
            "region_id": "aws-eu-west-1",
        },
    )

    yield created_project

    print("Deleting project")
    serverless_api("DELETE", f"/api/v1/serverless/projects/elasticsearch/{created_project['id']}")


@pytest.fixture(scope="session")
def serverless_project_config(serverless_project):
    credentials = serverless_api(
        "POST",
        f"/api/v1/serverless/projects/elasticsearch/{serverless_project['id']}{GET_CREDENTIALS_ENDPOINT}",
    )

    es_endpoint = serverless_project["endpoints"]["elasticsearch"]
    es_hostname = urllib.parse.urlparse(es_endpoint).hostname
    rally_target_host = f"{es_hostname}:443"

    print("Waiting for DNS propagation")
    for _ in range(90):
        time.sleep(10)
        with contextlib.suppress(subprocess.CalledProcessError):
            subprocess.run(["nslookup", es_hostname, "8.8.8.8"], check=True)
            break
    else:
        raise ValueError("Timed out waiting for DNS propagation")

    print("Waiting for Elasticsearch")
    for _ in range(30):
        try:
            es = Elasticsearch(
                f"https://{rally_target_host}",
                basic_auth=(
                    credentials["username"],
                    credentials["password"],
                ),
                request_timeout=10,
            )
            info = es.info()
            print("GET /")
            print(json.dumps(info.body, indent=2))

            authenticate = es.perform_request(method="GET", path="/_security/_authenticate")
            print("GET /_security/_authenticate")
            print(json.dumps(authenticate.body, indent=2))

            break
        except Exception as e:
            print(f"GET / Failed with {type(e)}")
            time.sleep(10)
    else:
        raise ValueError("Timed out waiting for Elasticsearch")

    # Create API key to test Rally with a public user
    api_key = es.security.create_api_key(name="public-api-key")

    yield ServerlessProjectConfig(
        rally_target_host,
        credentials["username"],
        credentials["password"],
        api_key.body["encoded"],
    )
