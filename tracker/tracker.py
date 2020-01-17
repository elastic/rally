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
import logging
import os
import pathlib

from elasticsearch import Elasticsearch
from jinja2 import Environment, PackageLoader

from esrally import log
from tracker import corpus, index

TRACK_TEMPLATES = {
    "track.json.j2": "track.json",
    "operations.json.j2": "operations/default.json",
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
    parser.add_argument("--index", required=True, help="Index to create track for")
    parser.add_argument("--trackname", help="Name of track to use, if different from the name of index")
    parser.add_argument("--outdir", help="Output directory for track (default: tracks/)")

    args = parser.parse_args()
    if not args.trackname:
        args.trackname = args.index
    if not args.outdir:
        args.outdir = os.path.join(os.getcwd(), "tracks")
    return args


def main():
    log.configure_logging()
    args = get_args()

    client = Elasticsearch(hosts=args.hosts)
    info = client.info()
    logging.info("Connected to %s version %s", info['name'], info['version']['number'])

    outpath = os.path.join(args.outdir, args.trackname)
    if not os.path.exists(outpath):
        pathlib.Path(outpath).mkdir(parents=True)

    index.extract(client, outpath, args.index)

    template_vars = {
        "index_name": args.index,
        "track_name": args.trackname
    }
    corpus_vars = corpus.extract(client, outpath, args.index)
    template_vars.update(corpus_vars)

    for template, dest_filename in TRACK_TEMPLATES.items():
        dest_path = os.path.join(outpath, dest_filename)
        process_template(template, template_vars, dest_path)

    logging.info("%s has been tracked!", args.trackname)


if __name__ == '__main__':
    main()
