import logging

import esrally.storage
from esrally import config

MIRRORS_FILES = "~/.rally/storage-mirrors.json"
GET_FILES = [
    "https://rally-tracks.elastic.co/apm/span.json.bz2",
    "https://rally-tracks.elastic.co/behavioral_analytics/events.json.bz2",
]


def main():
    logging.basicConfig(level=logging.INFO)
    cfg = config.Config()
    cfg.add(config.Scope.application, "storage", "storage.mirrors_files", MIRRORS_FILES)
    manager = esrally.storage.Manager.from_config(cfg)
    trs = []
    for url in GET_FILES:
        trs.append(manager.get(url))
    try:
        for tr in trs:
            tr.wait()
    except KeyboardInterrupt:
        pass
    finally:
        manager.shutdown()


if __name__ == "__main__":
    main()
