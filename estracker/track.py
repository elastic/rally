import logging
import os
import pathlib

from elasticsearch import ElasticsearchException
from jinja2 import Environment, PackageLoader

from esrally.utils import io
from estracker import corpus, index


TRACK_TEMPLATES = {
    "track.json.j2": "track.json",
    "challenges.json.j2": "challenges/default.json",
}


def process_template(template_filename, template_vars, dest_path):
    env = Environment(loader=PackageLoader("estracker", "templates"))
    template = env.get_template(template_filename)

    parent_dir = pathlib.Path(dest_path).parent
    io.ensure_dir(str(parent_dir))

    with open(dest_path, "w") as outfile:
        outfile.write(template.render(template_vars))


def extract_mappings_and_corpora(client, outpath, indices_to_extract):
    logger = logging.getLogger(__name__)

    indices = []
    corpora = []
    for index_name in indices_to_extract:
        try:
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

    template_vars = {
        "track_name": track_name,
        "indices": indices,
        "corpora": corpora
    }

    for template, dest_filename in TRACK_TEMPLATES.items():
        dest_path = os.path.join(outpath, dest_filename)
        process_template(template, template_vars, dest_path)
