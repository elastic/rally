import json
import logging
from esrally.utils import console

from elasticsearch.client import _normalize_hosts

def csv_to_list(csv):
    if csv is None:
        return None
    elif len(csv.strip()) == 0:
        return []
    else:
        return [e.strip() for e in csv.split(",")]


class TargetHosts:
    """
    --target-hosts can provide either "ip:port,ip:port" or a stringified dict of clusters and hosts e.g.:
    '{"default": "ip1:port1,ip2:port2", "remote_1": "ip2:port2", "remote_2": "ip3:port3,ip4:port4"}'
    Provide some helper methods to return the parsed list, if using the usual definition of targetting one cluster,
    or the list of target hosts labelled as "default" if there are >1 groups.
    """

    EXAMPLE_USAGE = """--target-hosts='{"default": "ip1:port1,ip2:port2", "remote_1": "ip2:port2", "remote_2": "ip3:port3,ip4:port4"}'"""
    LOCAL_TARGET_HOSTS_GROUP = 'default'
    REMOTE_TARGET_HOSTS_GROUP = 'remote'

    def __init__(self, arg):
        self.__hosts = arg
        self.__all_hosts = None

        self.parse_target_hosts()

    def __call__(self):
        """Return the targets hosts assigned to the 'default' key"""

        return self.default

    def __getitem__(self, key):
        """
        Race expects the cfg object to be subscriptable
        Just return 'default'
        """

        return self.default

    @property
    def default(self):
        return self.__all_hosts['default']

    @property
    def all_hosts(self):
        return self.__all_hosts

    def parse_target_hosts(self):
        try:
            hosts_dict = json.loads(self.__hosts)

            if len(hosts_dict) == 0:
                # raise error here
                pass
            elif len(hosts_dict) == 1 and 'default' not in hosts_dict.keys():
                console.warn(
                    "--target-hosts declares only one cluster but is not using the 'default' key.\n"
                    "Silently assuming this the 'default' cluster.\n"
                    "If you are targetting a single cluster, specifying `--target-hosts=\"ip1:port1,ip2:port2\"` should be enough.\n"
                    "Otherwise, in the future, please define your clusters like:\n"
                    "{}".format(TargetHosts.EXAMPLE_USAGE)
                )

                self.__all_hosts = {'default': _normalize_hosts(hosts_dict[list(hosts_dict)[0]])}

            elif len(hosts_dict) > 1 and 'default' not in hosts_dict.keys():
                console.println(
                    "You have specified a number of clusters with --target-hosts "
                    "but none has the \"default\" key. Please adjust your --target-hosts parameter using the following example: \n"
                    "{}\nand try again.".format(TargetHosts.EXAMPLE_USAGE))
                exit(1)
            else:
                self.__all_hosts = { key: _normalize_hosts(value)
                                     for key,value in hosts_dict.items() }

        except json.decoder.JSONDecodeError:
            self.__all_hosts = {'default' : _normalize_hosts(csv_to_list(self.__hosts)) }
