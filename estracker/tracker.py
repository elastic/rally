# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import json
import logging
from logging import config
import os

from esrally import version
from esrally.client import EsClientFactory
from esrally.utils import opts, io
from esrally.utils.opts import csv_to_list
from estracker import track


def list_type(arg):
    lst = csv_to_list(arg)
    if len(lst) < 1:
        raise argparse.ArgumentError("At least one argument required!")
    return lst


def get_arg_parser():
    parser = argparse.ArgumentParser(description="Dump mappings and document-sources from an Elasticsearch index to create a Rally track.")

    parser.add_argument("--target-hosts", default="", required=True, help="Elasticsearch host(s) to connect to")
    parser.add_argument("--client-options", default=opts.ClientOptions.DEFAULT_CLIENT_OPTIONS, help="Elasticsearch client options")
    parser.add_argument("--indices", type=list_type, required=True, help="Indices to include in track")
    parser.add_argument("--track-name", help="Name of track to use, if different from the name of index")
    parser.add_argument("--outdir", default=os.path.join(os.getcwd(), "tracks"), help="Output directory for track (default: tracks/)")
    parser.add_argument('--version', action='version', version="%(prog)s " + version.version())

    return parser


def load_json(p):
    with open(p) as f:
        return json.load(f)


def configure_logging():
    log_config_path = os.path.join(os.path.dirname(__file__), "resources", "logging.json")
    log_conf = load_json(log_config_path)
    config.dictConfig(log_conf)
    logging.captureWarnings(True)


def main():
    configure_logging()
    logger = logging.getLogger(__name__)
    argparser = get_arg_parser()
    args = argparser.parse_args()

    if not args.track_name:
        if len(args.indices) == 1:
            args.track_name = args.indices[0]
        else:
            raise ValueError("Track name must be specified when including multiple indices")

    target_hosts = opts.TargetHosts(args.target_hosts)
    client_options = opts.ClientOptions(args.client_options, target_hosts=target_hosts)
    client = EsClientFactory(hosts=target_hosts.all_hosts[opts.TargetHosts.DEFAULT],
                             client_options=client_options.all_client_options[opts.TargetHosts.DEFAULT]).create()

    info = client.info()
    logging.info("Connected to Elasticsearch %s version %s", info["name"], info["version"]["number"])

    outpath = os.path.join(args.outdir, args.track_name)
    abs_outpath = os.path.abspath(outpath)
    io.ensure_dir(abs_outpath)

    track.extract(client, abs_outpath, args.track_name, args.indices)

    logger.info("%s has been tracked!", args.track_name)
    logger.info("To run with Rally: esrally race --track-path=%s", abs_outpath)


if __name__ == '__main__':
    main()
