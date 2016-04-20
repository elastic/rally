from unittest import TestCase

from esrally import config, exceptions
from esrally.mechanic import launcher


class ExternalLauncherTests(TestCase):
    def test_setup_external_cluster_single_node(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "launcher", "external.target.hosts", ["localhost:9200"])

        m = launcher.ExternalLauncher(cfg)
        cluster = m.start(None, None, None)

        self.assertEqual(cluster.hosts, [{"host": "localhost", "port": "9200"}])

    def test_setup_external_cluster_multiple_nodes(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "launcher", "external.target.hosts",
                ["search.host-a.internal:9200", "search.host-b.internal:9200"])

        m = launcher.ExternalLauncher(cfg)
        cluster = m.start(None, None, None)

        self.assertEqual(cluster.hosts,
                         [{"host": "search.host-a.internal", "port": "9200"}, {"host": "search.host-b.internal", "port": "9200"}])

    def test_raise_system_setup_exception_on_invalid_list(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "launcher", "external.target.hosts",
                ["search.host-a.internal", "search.host-b.internal:9200"])

        m = launcher.ExternalLauncher(cfg)
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            m.start(None, None, None)
            self.assertTrue("Could not initialize external cluster. Invalid format for [search.host-a.internal, "
                            "search.host-b.internal:9200]. Expected a comma-separated list of host:port pairs, "
                            "e.g. host1:9200,host2:9200." in ctx.exception)

