import logging

import esrally.storage
from esrally import config


def main():
    logging.basicConfig(level=logging.INFO)
    manager = esrally.storage.Manager.from_config(config.Config())
    try:
        # tr1 = manager.get("https://rally-tracks.elastic.co/apm/span.json.bz2")
        tr2 = manager.get("https://rally-tracks.elastic.co/behavioral_analytics/events.json.bz2")
        # tr3 = manager.get("https://ftp.rediris.es/mirror/ubuntu-releases/24.04.2/ubuntu-24.04.2-live-server-amd64.zip")
        # tr1.wait()
        tr2.wait()
        # tr3.wait()
    except KeyboardInterrupt:
        pass
    finally:
        manager.shutdown()


main()
