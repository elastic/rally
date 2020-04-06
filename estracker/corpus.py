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

from esrally.utils import console


DOCS_COMPRESSOR = bz2.BZ2Compressor
COMP_EXT = ".bz2"


def template_vars(index_name, out_path, doc_count):
    comp_outpath = out_path + COMP_EXT
    return {
        "index_name": index_name,
        "filename": os.path.basename(comp_outpath),
        "path": comp_outpath,
        "doc_count": doc_count,
        "uncompressed_bytes": os.path.getsize(out_path),
        "compressed_bytes": os.path.getsize(comp_outpath)
    }


def get_doc_outpath(outdir, name, suffix=""):
    return os.path.join(outdir, "{}-documents{}.json".format(name, suffix))


def extract(client, outdir, index):
    """
    Scroll an index with a match-all query, dumping document source to
    outdir/documents.json
    :param client: Elasitcsearch client to scroll
    :param outdir: Destination directory for corpus dump
    :param index: Name of index to dump
    :return: dict of properties describing the corpus for templates
    """

    logger = logging.getLogger(__name__)
    outpath = get_doc_outpath(outdir, index)

    total_docs = client.count(index=index)["count"]
    logger.info("%d total docs in index %s", total_docs, index)

    if total_docs == 0:
        raise ValueError("Cannot dump an index with 0 documents!")

    if total_docs >= 1000:
        tm_outpath = get_doc_outpath(outdir, index, "-1k")
        dump_documents(client, index, tm_outpath, 1000)
    else:
        logger.warning("Insufficient document count for test-mode corpus!")
    dump_documents(client, index, outpath, total_docs)
    return template_vars(index, outpath, total_docs)


def dump_documents(client, index, outpath, total_docs):
    from elasticsearch import helpers

    logger = logging.getLogger(__name__)
    freq = max(1, total_docs // 1000)

    progress = console.progress()
    compressor = DOCS_COMPRESSOR()
    comp_outpath = outpath + COMP_EXT
    with open(outpath, "wb") as outfile:
        with open(comp_outpath, "wb") as comp_outfile:
            logger.info("Now dumping corpus to %s...", outpath)

            query = {"query": {"match_all": {}}}
            for n, doc in enumerate(helpers.scan(client, query=query, index=index)):
                if n > total_docs:
                    break
                docsrc = doc["_source"]
                data = (json.dumps(docsrc, separators=(',', ':')) + "\n").encode("utf-8")

                outfile.write(data)
                comp_outfile.write(compressor.compress(data))

                render_progress(progress, index, n + 1, total_docs, freq)

            comp_outfile.write(compressor.flush())
    progress.finish()


def render_progress(progress, index, cur, total, freq):
    if cur % freq == 0 or total - cur < freq:
        percent = (cur * 100) / total
        progress.print(f"Extracting documents for index {index}...", f"{cur}/{total} ({percent:.1f}%)")


def purge(outpath, index_name):
    for suffix in ("", "-1k"):
        path = os.path.join(outpath, index_name, suffix)
        os.unlink(path)
