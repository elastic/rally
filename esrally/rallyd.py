import sys
import time
import logging
import argparse

from esrally import actor, version, exceptions, DOC_LINK, BANNER, PROGRAM_NAME
from esrally.utils import console


def start(args):
    if actor.actor_system_already_running():
        raise exceptions.RallyError("An actor system appears to be already running.")
    # TheSpian writes the following warning upon start (at least) on Mac OS X:
    #
    # WARNING:root:Unable to get address info for address 103.1.168.192.in-addr.arpa (AddressFamily.AF_INET,\
    # SocketKind.SOCK_DGRAM, 17, 0): <class 'socket.gaierror'> [Errno 8] nodename nor servname provided, or not known
    #
    # Therefore, we will not show warnings but only errors.
    logging.basicConfig(level=logging.ERROR)
    actor.bootstrap_actor_system(local_ip=args.node_ip, coordinator_ip=args.coordinator_ip)
    console.info("Successfully started actor system on node [%s] with coordinator node IP [%s]." % (args.node_ip, args.coordinator_ip))


def stop(raise_errors=True):
    if actor.actor_system_already_running():
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


def main():
    console.init()

    parser = argparse.ArgumentParser(prog=PROGRAM_NAME,
                                     description=BANNER + "\n\n Rally daemon to support remote benchmarks",
                                     epilog="Find out more about Rally at %s" % console.format.link(DOC_LINK),
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--version', action='version', version="%(prog)s " + version.version())

    subparsers = parser.add_subparsers(
        title="subcommands",
        dest="subcommand",
        help="")
    subparsers.required = True

    start_command = subparsers.add_parser("start", help="Starts the Rally daemon")
    restart_command = subparsers.add_parser("restart", help="Restarts the Rally daemon")
    for p in [start_command, restart_command]:
        p.add_argument(
            "--node-ip",
            help="The IP of this node.")
        p.add_argument(
            "--coordinator-ip",
            help="The IP of the coordinator node."
        )
    subparsers.add_parser("stop", help="Stops the Rally daemon")

    args = parser.parse_args()

    if args.subcommand == "start":
        start(args)
    elif args.subcommand == "stop":
        stop()
    elif args.subcommand == "restart":
        stop(raise_errors=False)
        start(args)
    else:
        raise exceptions.RallyError("Unknown subcommand [%s]" % args.subcommand)


if __name__ == '__main__':
    main()
