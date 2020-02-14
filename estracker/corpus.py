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

import bz2
import json
import logging
import os
import pathlib

from esrally.utils import console


def template_vars(index_name, out_path, comp_outpath, doc_count):
    corpus_path = pathlib.Path(out_path)
    compressed_corpus_path = pathlib.Path(comp_outpath)
    return {
        "index_name": index_name,
        "base_url": pathlib.Path(out_path).parent.as_uri(),
        "filename": corpus_path.name,
        "path": corpus_path,
        "doc_count": doc_count,
        "uncompressed_bytes": os.path.getsize(out_path),
        "compressed_bytes": os.path.getsize(comp_outpath)
    }


def extract(client, outdir, index):
    """
    Scroll an index with a match-all query, dumping document source to
    outdir/documents.json
    :param client: Elasitcsearch client to scroll
    :param outdir: Destination directory for corpus dump
    :param index: Name of index to dump
    :return: dict of properties describing the corpus for templates
    """
    from elasticsearch import helpers

    logger = logging.getLogger(__name__)
    outpath = os.path.join(outdir, "{}-documents.json".format(index))

    total_docs = client.count(index=index)["count"]
    logger.info("%d total docs in index %s", total_docs, index)
    freq = max(1, total_docs // 1000)

    progress = console.progress()

    compressor = bz2.BZ2Compressor()
    comp_outpath = outpath + ".bz2"

    with open(outpath, "wb") as outfile:
        with open(comp_outpath, "wb") as comp_outfile:
            logger.info("Now dumping corpus to %s...", outpath)

            query = {"query": {"match_all": {}}}
            for n, doc in enumerate(helpers.scan(client, query=query, index=index)):
                docsrc = doc["_source"]
                data = (json.dumps(docsrc, separators=(',', ':')) + "\n").encode("utf-8")

                outfile.write(data)
                comp_outfile.write(compressor.compress(data))

                render_progress(progress, n+1, total_docs, freq)

            comp_outfile.write(compressor.flush())
    progress.finish()
    return template_vars(index, outpath, comp_outpath, total_docs)


def render_progress(progress, cur, total, freq):
    if cur % freq == 0 or total - cur < freq:
        percent = (cur * 100) / total
        progress.print("Extracting documents...", "{n}/{total_docs} ({percent:.1f}%)".format(n=cur, total_docs=total, percent=percent))
