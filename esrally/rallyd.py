# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
The Rally daemon: a thin wrapper around ``ray start`` and ``ray stop``.

The daemon on the coordinator node is the head of a Ray cluster that listens on ``actor.RAY_GCS_PORT``. Daemons on other
nodes join that cluster. Rally actors are then placed on nodes by IP address.
"""

import argparse
import logging
import os
import subprocess
import sys

from esrally import (
    BANNER,
    PROGRAM_NAME,
    actor,
    check_python_version,
    doc_link,
    exceptions,
    log,
    version,
)
from esrally.utils import console, net, process

LOG = logging.getLogger(__name__)


def ray_command(*args: str) -> list[str]:
    """
    Command line to run Ray's command line tool with the same Python interpreter (and thus virtual environment) as Rally.
    """
    return [sys.executable, "-m", "ray.scripts.scripts", *args]


def run_ray(*args: str, capture_output: bool = True) -> int:
    """
    Runs Ray's command line tool. Its output is written to Rally's log and shown on the console only if it fails.
    """
    actor.configure_ray_environment()
    command = ray_command(*args)
    LOG.info("Running %s", command)
    if not capture_output:
        return subprocess.run(command, check=False, env=os.environ.copy()).returncode
    completed = subprocess.run(command, check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=os.environ.copy())
    for line in completed.stdout.splitlines():
        LOG.info("ray: %s", line)
    if completed.returncode != 0:
        console.println(completed.stdout, force=True)
    return completed.returncode


def auth_token_path() -> str:
    return os.environ.get("RAY_AUTH_TOKEN_PATH") or os.path.join(os.path.expanduser("~"), ".ray", "auth_token")


def has_auth_token() -> bool:
    return bool(os.environ.get("RAY_AUTH_TOKEN")) or os.path.isfile(auth_token_path())


def auth_disabled() -> bool:
    return os.environ.get("RAY_AUTH_MODE", "").lower() == "disabled"


def start(args):
    if actor.is_daemon_running_locally():
        raise exceptions.RallyError("A Rally daemon appears to be already running on this machine. Stop it with `esrallyd stop`.")
    node_ip = net.resolve(args.node_ip) or args.node_ip
    coordinator_ip = net.resolve(args.coordinator_ip) or args.coordinator_ip
    is_coordinator = node_ip == coordinator_ip

    if not auth_disabled():
        if is_coordinator:
            # All nodes and clients of the cluster authenticate with the same token. Generate it unless it exists.
            if run_ray("get-auth-token", "--generate") != 0:
                raise exceptions.RallyError("Could not generate an authentication token for the Rally daemon.")
        elif not has_auth_token():
            raise exceptions.RallyError(
                f"No authentication token found at [{auth_token_path()}]. Copy the file [~/.ray/auth_token] from the coordinator "
                f"node [{coordinator_ip}] to this machine and try again."
            )

    common = ["--node-ip-address", node_ip, "--disable-usage-stats"]
    # In Docker, keep the container running for as long as the daemon runs and show its output.
    block = console.RALLY_RUNNING_IN_DOCKER
    if block:
        common.append("--block")
        console.info(f"Starting Rally daemon on node [{node_ip}] with coordinator node IP [{coordinator_ip}].", force=True)
    if is_coordinator:
        args = ["start", "--head", "--port", str(actor.RAY_GCS_PORT), "--include-dashboard", "false", *common]
    else:
        args = ["start", "--address", f"{coordinator_ip}:{actor.RAY_GCS_PORT}", *common]
    returncode = run_ray(*args, capture_output=not block)
    if returncode != 0:
        raise exceptions.RallyError(f"Could not start the Rally daemon (`ray start` exited with code [{returncode}]).")
    console.info(f"Successfully started Rally daemon on node [{node_ip}] with coordinator node IP [{coordinator_ip}].", force=True)


def stop(raise_errors=True):
    if not actor.is_daemon_running_locally():
        if raise_errors:
            console.error("Could not shut down Rally daemon: Rally daemon is not running.")
            sys.exit(1)
        return
    console.info("Shutting down Rally daemon.", force=True)
    returncode = run_ray("stop")
    if returncode != 0 and raise_errors:
        raise exceptions.RallyError(f"Could not shut down Rally daemon (`ray stop` exited with code [{returncode}]).")
    console.info("Rally daemon has been shut down.", force=True)


def status():
    if actor.is_daemon_running_locally():
        console.println("Running", force=True)
    else:
        console.println("Stopped", force=True)


def main():
    check_python_version()
    log.install_default_log_config()
    log.configure_logging()
    process.disable_os_log_on_macos()
    console.init(assume_tty=False)

    parser = argparse.ArgumentParser(
        prog=PROGRAM_NAME,
        description=BANNER + "\n\n Rally daemon to support remote benchmarks",
        epilog=f"Find out more about Rally at {console.format.link(doc_link())}",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--version", action="version", version="%(prog)s " + version.version())

    subparsers = parser.add_subparsers(title="subcommands", dest="subcommand", help="")
    subparsers.required = True

    start_command = subparsers.add_parser("start", help="Starts the Rally daemon")
    restart_command = subparsers.add_parser("restart", help="Restarts the Rally daemon")
    for p in [start_command, restart_command]:
        p.add_argument("--node-ip", required=True, help="The IP of this node.")
        p.add_argument("--coordinator-ip", required=True, help="The IP of the coordinator node.")
    subparsers.add_parser("stop", help="Stops the Rally daemon")
    subparsers.add_parser("status", help="Shows the current status of the local Rally daemon")

    args = parser.parse_args()

    try:
        if args.subcommand == "start":
            start(args)
        elif args.subcommand == "stop":
            stop()
        elif args.subcommand == "status":
            status()
        elif args.subcommand == "restart":
            stop(raise_errors=False)
            start(args)
        else:
            raise exceptions.RallyError("Unknown subcommand [%s]" % args.subcommand)
    except exceptions.RallyError as e:
        LOG.exception("esrallyd %s failed.", args.subcommand)
        console.error(e.full_message)
        sys.exit(1)


if __name__ == "__main__":
    main()
