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

from elasticsearch import Elasticsearch
from jinja2 import Environment, PackageLoader

from tracker import corpus, index

TRACK_TEMPLATES = {
    "track.json.j2": "track.json",
    "challenges.json.j2": "challenges/default.json",
}


def process_template(template_filename, template_vars, dest_path):
    env = Environment(loader=PackageLoader("tracker", "templates"))
    template = env.get_template(template_filename)

    parent_dir = pathlib.Path(dest_path).parent
    if not parent_dir.exists():
        parent_dir.mkdir()

    with open(dest_path, "w") as outfile:
        outfile.write(template.render(template_vars))


def get_args():
    parser = argparse.ArgumentParser(description="Dump mappings and document-sources from an Elasticsearch index to create a Rally track.")

    parser.add_argument("--hosts", nargs='+', required=True, help="Elasticsearch host(s) to connect to")
    parser.add_argument("--indices", nargs='+', required=True, help="Indices to include in track")
    parser.add_argument("--trackname", help="Name of track to use, if different from the name of index")
    parser.add_argument("--outdir", help="Output directory for track (default: tracks/)")

    args = parser.parse_args()
    if not args.trackname:
        args.trackname = args.index
    if not args.outdir:
        args.outdir = os.path.join(os.getcwd(), "tracks")
    return args


def load_json(p):
    with open(p) as f:
        return json.load(f)


def configure_logging():
    log_config_path = os.path.join(os.path.dirname(__file__), "resources", "logging.json")
    log_conf = load_json(log_config_path)
    logging.config.dictConfig(log_conf)
    logging.captureWarnings(True)


def main():
    configure_logging()
    args = get_args()

    client = Elasticsearch(hosts=args.hosts)
    info = client.info()
    logging.info("Connected to Elasticsearch %s version %s", info['name'], info['version']['number'])

    outpath = os.path.join(args.outdir, args.trackname)
    if not os.path.exists(outpath):
        pathlib.Path(outpath).mkdir(parents=True)

    template_vars = {
        "track_name": args.trackname
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

    logging.info("%s has been tracked!", args.trackname)


if __name__ == '__main__':
    main()
