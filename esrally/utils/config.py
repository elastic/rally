import json

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
    '{"default": "ip1:port1,ip2:port2", "remote": "ip3:port3"}'
    Provide some helper methods to return the parsed list, if using the usual definition of targetting one cluster,
    or the list of target hosts labelled as "default" if there are >1 groups.
    """

    LOCAL_TARGET_HOSTS_GROUP = 'default'
    REMOTE_TARGET_HOSTS_GROUP = 'remote'

    def __init__(self, arg):
        self.hosts = arg
        self.all_hosts = None

        self.parse_target_hosts()

    def parse_target_hosts(self):
        try:
            self.all_hosts = { key:_normalize_hosts(value)
                               for key,value in json.loads(self.hosts).items() }
        except json.decoder.JSONDecodeError:
            self.all_hosts = {'default' : _normalize_hosts(csv_to_list(self.hosts)) }
