import logging
import os
import pathlib

from elasticsearch import ElasticsearchException
from jinja2 import Environment, PackageLoader

from esrally.utils import io
from estracker import corpus, index


TRACK_TEMPLATES = {
    "track.json.j2": "track.json"
}


def process_template(template_filename, template_vars, dest_path):
    env = Environment(loader=PackageLoader("estracker", "templates"))
    template = env.get_template(template_filename)

    parent_dir = pathlib.Path(dest_path).parent
    io.ensure_dir(str(parent_dir))

    with open(dest_path, "w") as outfile:
        outfile.write(template.render(template_vars))


def check_index_name(index_name):
    if len(index_name) == 0:
        raise ValueError("Index name cannot be empty.")
    if index_name.startswith("."):
        raise ValueError("Cannot extract hidden indices.")


def extract_mappings_and_corpora(client, outpath, indices_to_extract):
    logger = logging.getLogger(__name__)

    indices = []
    corpora = []
    for index_name in indices_to_extract:
        try:
            check_index_name(index_name)
            corpus_vars = corpus.extract(client, outpath, index_name)
            corpora.append(corpus_vars)

            index_vars = index.extract(client, outpath, index_name)
            indices.append(index_vars)
        except (ValueError, ElasticsearchException) as e:
            logger.warning("Failed to extract %s: %s", index_name, e)
            # Handle extra value if we failed after corpus extraction
            if len(corpora) > len(indices):
                corpora.pop()
                corpus.purge(outpath, index_name)

    return indices, corpora


def extract(client, outpath, track_name, indices_to_extract):
    indices, corpora = extract_mappings_and_corpora(client, outpath, indices_to_extract)
    if len(indices) == 0:
        raise RuntimeError("Failed to extract any indices for track!")

    template_vars = {
        "track_name": track_name,
        "indices": indices,
        "corpora": corpora
    }

    dest_path = os.path.join(outpath, "track.json")
    process_template("track.json.j2", template_vars, dest_path)
