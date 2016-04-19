from unittest import TestCase

from esrally import config
from esrally.track import track


class MarshalTests(TestCase):
    def test_adds_distribution_version_to_mapping(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "source", "distribution.version", "5.0.0")

        t = track.Type("test", "test-mapping.json")

        marshal = track.Marshal(cfg)
        self.assertEqual(marshal.mapping_file_name(t), "test-mapping-5.0.0.json")

    def test_no_distribution_version_for_source_distro(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "source", "distribution.version", "")

        t = track.Type("test", "test-mapping.json")

        marshal = track.Marshal(cfg)
        self.assertEqual(marshal.mapping_file_name(t), "test-mapping.json")
