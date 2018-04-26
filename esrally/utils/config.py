import argparse
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


class CfgStringifiedOptions:
    """
    Base Class to help either parsing --target-hosts or --client-options
    TODO: read parameters from a specified file
    """

    def __init__(self, argname, argvalue, example_usage, parser=_normalize_hosts):
        self.__argname = argname
        self.__options = argvalue
        self.__parsed_options = None
        self.example_usage = example_usage
        self.parser = parser

        self.parse_arg()

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
        return self.__parsed_options['default']

    @property
    def all_options(self):
        """Return a dict with all parsed options"""
        return self.__parsed_options

    def parse_arg(self):
        try:
            options_dict = json.loads(self.__options)

            if len(options_dict) == 0:
                raise argparse.ArgumentTypeError("cli argument {} must not be empty JSON object".format(self.__argname))
            elif len(options_dict) == 1 and 'default' not in options_dict.keys():
                console.warn(
                    "cli argument {} declares only one cluster but is not using the 'default' key.\n"
                    "Silently assuming this the 'default' cluster.\n"
                    "If you are targetting a single cluster, specifying `--target-hosts=\"ip1:port1,ip2:port2\"` should be enough.\n"
                    "Otherwise, in the future, please define your clusters like:\n"
                    "{}".format(self.__argname,self.example_usage)
                )

                self.__parsed_options = {'default': _normalize_hosts(options_dict[list(options_dict)[0]])}

            elif len(options_dict) > 1 and 'default' not in options_dict.keys():
                console.println(
                    "You have specified a number of clusters with {0} "
                    "but none has the \"default\" key. Please adjust your {0} parameter using the following example: \n"
                    "{1}\nand try again.".format(self.__argname, self.example_usage))
                exit(1)
            else:
                self.__parsed_options = { key: self.parser(csv_to_list(value))
                                          for key,value in options_dict.items() }

        except json.decoder.JSONDecodeError:
            self.__parsed_options = {'default' : self.parser(csv_to_list(self.__options)) }


class TargetHosts(CfgStringifiedOptions):
    def __init__(self, argname, argvalue):
        example_usage = argname+"="+ \
        """'{"default": "ip1:port1,ip2:port2", "remote_1": "ip2:port2", "remote_2": "ip3:port3,ip4:port4"]}'"""
        super().__init__(
            argname,
            argvalue,
            example_usage,
            _normalize_hosts
        )

    @property
    def all_hosts(self):
        """Return a dict with all parsed options"""
        return self.all_options


class ClientOptions(CfgStringifiedOptions):
    def __init__(self, argname, argvalue):
        example_usage = argname+"="+ \
                        '"{\"default\": \"timeout:60\",'\
                        '\"remote_1\": \"use_ssl:true,verify_certs:true,basic_auth_user:\'elastic\',basic_auth_password:\'changeme\'\"'\
                        ',\"remote_2\": \"use_ssl:true,verify_certs:true,ca_certs:\'/path/to/cacert.pem\'\"}"'
        super().__init__(
            argname,
            argvalue,
            example_usage,
            kv_to_map
        )

    @property
    def all_client_options(self):
        """Return a dict with all parsed options"""
        return self.all_options
