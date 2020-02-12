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
import pathlib

from jinja2 import Environment, PackageLoader

from esrally import version
from esrally.client import EsClientFactory
from esrally.utils import opts, io
from estracker import corpus, index

TRACK_TEMPLATES = {
    "track.json.j2": "track.json",
    "challenges.json.j2": "challenges/default.json",
}


def process_template(template_filename, template_vars, dest_path):
    env = Environment(loader=PackageLoader("tracker", "templates"))
    template = env.get_template(template_filename)

    parent_dir = pathlib.Path(dest_path).parent
    io.ensure_dir(parent_dir)

    with open(dest_path, "w") as outfile:
        outfile.write(template.render(template_vars))


def get_arg_parser():
    parser = argparse.ArgumentParser(description="Dump mappings and document-sources from an Elasticsearch index to create a Rally track.")

    parser.add_argument("--target-hosts", default="", required=True, help="Elasticsearch host(s) to connect to")
    parser.add_argument("--client-options", default=opts.ClientOptions.DEFAULT_CLIENT_OPTIONS, help="Elasticsearch client options")
    parser.add_argument("--indices", nargs="+", required=True, help="Indices to include in track")
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
    argparser = get_arg_parser()
    args = argparser.parse_args()

    if not args.track_name:
        args.track_name = args.indices[0]

    target_hosts = opts.TargetHosts(args.target_hosts)
    client_options = opts.ClientOptions(args.client_options, target_hosts=target_hosts)
    client = EsClientFactory(hosts=target_hosts.all_hosts[opts.TargetHosts.DEFAULT],
                             client_options=client_options.all_client_options[opts.TargetHosts.DEFAULT]).create()

    info = client.info()
    logging.info("Connected to Elasticsearch %s version %s", info["name"], info["version"]["number"])

    outpath = os.path.join(args.outdir, args.track_name)
    abs_outpath = os.path.abspath(outpath)
    io.ensure_dir(abs_outpath)

    template_vars = {
        "track_name": args.track_name
    }

    indices = []
    corpora = []
    for index_name in args.indices:
        index_vars = index.extract(client, outpath, index_name)
        indices.append(index_vars)

        corpus_vars = corpus.extract(client, outpath, index_name)
        corpora.append(corpus_vars)

    template_vars.update({
        "indices": indices,
        "corpora": corpora
    })
    for template, dest_filename in TRACK_TEMPLATES.items():
        dest_path = os.path.join(outpath, dest_filename)
        process_template(template, template_vars, dest_path)

    logging.info("%s has been tracked!", args.track_name)
    logging.info("To run with Rally: esrally race --track-path=%s", abs_outpath)


if __name__ == '__main__':
    main()
