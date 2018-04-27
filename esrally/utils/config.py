import argparse
import json
import logging
from esrally.utils import console, io

from elasticsearch.client import _normalize_hosts


def csv_to_list(csv):
    if csv is None:
        return None
    elif len(csv.strip()) == 0:
        return []
    else:
        return [e.strip() for e in csv.split(",")]


def to_bool(v):
    if v is None:
        return None
    elif v.lower() == "false":
        return False
    elif v.lower() == "true":
        return True
    else:
        raise ValueError("Could not convert value '%s'" % v)


def kv_to_map(kvs):
    def convert(v):
        # string
        if v.startswith("'"):
            return v[1:-1]

        # int
        try:
            return int(v)
        except ValueError:
            pass

        # float
        try:
            return float(v)
        except ValueError:
            pass

        # boolean
        return to_bool(v)

    result = {}
    for kv in kvs:
        k, v = kv.split(":")
        # key is always considered a string, value needs to be converted
        result[k.strip()] = convert(v.strip())
    return result


def to_dict(arg, default_parser=kv_to_map):
    if io.has_extension(arg, ".json"):
        with open(io.normalize_path(arg), mode="rt", encoding="utf-8") as f:
            return json.load(f)
    elif arg.startswith("{"):
        return json.loads(arg)
    else:
        return default_parser(csv_to_list(arg))


class CfgESConnectionOptions:
    """
    Base Class to help either parsing --target-hosts or --client-options
    TODO: read parameters from a specified file
    """

    def __call__(self):
        """Return a list with the options assigned to the 'default' key"""
        return self.default

    def __getitem__(self, key):
        """
        Race expects the cfg object to be subscriptable
        Just return 'default'
        """
        return self.default

    @property
    def default(self):
        """Return a list with the options assigned to the 'default' key"""
        return self.parsed_options["default"]

    @property
    def all_options(self):
        """Return a dict with all parsed options"""
        return self.parsed_options


class TargetHosts(CfgESConnectionOptions):
    def __init__(self, argvalue):
        self.argname = "--target-hosts"
        self.argvalue = argvalue
        self.parsed_options = []

        # example_usage = argname+"="+ \
        # """'{"default": "ip1:port1,ip2:port2", "remote_1": "ip2:port2", "remote_2": "ip3:port3,ip4:port4"]}'"""

        self.parse_options()

    def parse_options(self):
        def normalize_to_dict(arg):
            """
            Return parsed comma separated host string as dict with "default" key
            This is needed to support backwards compatible --target-hosts for single clusters that are not
            defined as a json string or file.
            """

            return {"default": _normalize_hosts(arg)}

        self.parsed_options = to_dict(self.argvalue, default_parser=normalize_to_dict)


    @property
    def all_hosts(self):
        """Return a dict with all parsed options"""
        return self.all_options


class ClientOptions(CfgESConnectionOptions):
    def __init__(self, argvalue):
        self.argname = "--client-options"
        self.argvalue = argvalue
        self.parsed_options = []

        # example_usage = argname+"="+ \
        #                 '"{\"default\": \"timeout:60\",'\
        #                 '\"remote_1\": \"use_ssl:true,verify_certs:true,basic_auth_user:\'elastic\',basic_auth_password:\'changeme\'\"'\
        #                 ',\"remote_2\": \"use_ssl:true,verify_certs:true,ca_certs:\'/path/to/cacert.pem\'\"}"'

        self.parse_options()

    def parse_options(self):
        def normalize_to_dict(arg):
            """
            Return parsed csv client options string as dict with "default" key
            This is needed to support backwards compatible --client-options for single clusters that are not
            defined as a json string or file.
            """

            return {"default": kv_to_map(arg)}

        self.parsed_options = to_dict(self.argvalue, default_parser=normalize_to_dict)


    @property
    def all_client_options(self):
        """Return a dict with all client options"""
        return self.all_options
