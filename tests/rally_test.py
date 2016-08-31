from unittest import TestCase

from esrally import exceptions, rally


class RallyTests(TestCase):
    def test_can_convert_valid_list(self):
        hosts = rally.convert_hosts(["search.host-a.internal:9200", "search.host-b.internal:9200"])
        self.assertEqual([{"host": "search.host-a.internal", "port": "9200"}, {"host": "search.host-b.internal", "port": "9200"}], hosts)

    def test_raise_system_setup_exception_on_invalid_list(self):
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            rally.convert_hosts(["search.host-a.internal", "search.host-b.internal:9200"])
            self.assertTrue("Could not convert hosts. Invalid format for [search.host-a.internal, "
                            "search.host-b.internal:9200]. Expected a comma-separated list of host:port pairs, "
                            "e.g. host1:9200,host2:9200." in ctx.exception)

    def test_csv_to_list(self):
        self.assertEqual([], rally.csv_to_list(""))
        self.assertEqual(["a", "b", "c", "d"], rally.csv_to_list("    a,b,c   , d"))
        self.assertEqual(["a-;d", "b", "c", "d"], rally.csv_to_list("    a-;d    ,b,c   , d"))

    def test_kv_to_map(self):
        self.assertEqual({}, rally.kv_to_map([]))
        self.assertEqual({"k": "v"}, rally.kv_to_map(["k:'v'"]))
        self.assertEqual({"k": "v", "size": 4, "empty": False, "temperature": 0.5},
                         rally.kv_to_map(["k:'v'", "size:4", "empty:false", "temperature:0.5"]))
