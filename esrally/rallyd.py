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

import argparse
import logging
import sys
import time

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
from esrally.utils import console


def start(args):
    if actor.actor_system_already_running():
        raise exceptions.RallyError("An actor system appears to be already running.")
    actor.bootstrap_actor_system(local_ip=args.node_ip, coordinator_ip=args.coordinator_ip)
    console.info("Successfully started actor system on node [%s] with coordinator node IP [%s]." % (args.node_ip, args.coordinator_ip))


def stop(raise_errors=True):
    if actor.actor_system_already_running():
        # noinspection PyBroadException
        try:
            # TheSpian writes the following warning upon start (at least) on Mac OS X:
            #
            # WARNING:root:Unable to get address info for address 103.1.168.192.in-addr.arpa (AddressFamily.AF_INET,\
            # SocketKind.SOCK_DGRAM, 17, 0): <class 'socket.gaierror'> [Errno 8] nodename nor servname provided, or not known
            #
            # Therefore, we will not show warnings but only errors.
            logging.basicConfig(level=logging.ERROR)
            running_system = actor.bootstrap_actor_system(try_join=True)
            running_system.shutdown()
            # await termination...
            console.info("Shutting down actor system.", end="", flush=True)
            while actor.actor_system_already_running():
                console.println(".", end="", flush=True)
                time.sleep(1)
            console.println(" [OK]")
        except BaseException:
            console.error("Could not shut down actor system.")
            if raise_errors:
                # raise again so user can see the error
                raise
    elif raise_errors:
        console.error("Could not shut down actor system: Actor system is not running.")
        sys.exit(1)


def status():
    if actor.actor_system_already_running():
        console.println("Running")
    else:
        console.println("Stopped")


def main():
    check_python_version()
    log.install_default_log_config()
    log.configure_logging()
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


if __name__ == "__main__":
    main()
