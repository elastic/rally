import sys
import logging
import argparse

from esrally import actor, version, exceptions, DOC_LINK, BANNER, PROGRAM_NAME
from esrally.utils import console


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

    start = subparsers.add_parser("start", help="Starts the Rally daemon")
    start.add_argument(
        "--node-ip",
        help="The IP of this node.")
    start.add_argument(
        "--coordinator-ip",
        help="The IP of the coordinator node."
    )
    subparsers.add_parser("stop", help="Stops the Rally daemon")

    args = parser.parse_args()

    if args.subcommand == "start":
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
        console.println("Successfully started actor system on node [%s]." % args.node_ip)
    elif args.subcommand == "stop":
        if actor.actor_system_already_running():
            try:
                running_system = actor.bootstrap_actor_system(try_join=True)
                running_system.shutdown()
                console.println("Successfully shut down actor system.")
            except BaseException:
                console.error("Could not shut down actor system.")
                # raise again so user can see the error
                raise
        else:
            console.error("Could not shut down actor system: Actor system is not running.")
            sys.exit(1)
    else:
        raise exceptions.RallyError("Unknown subcommand [%s]" % args.subcommand)


if __name__ == '__main__':
    main()
