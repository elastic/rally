from unittest import TestCase

from esrally import exceptions, rally


class RallyTests(TestCase):
    def test_csv_to_list(self):
        self.assertEqual([], rally.csv_to_list(""))
        self.assertEqual(["a", "b", "c", "d"], rally.csv_to_list("    a,b,c   , d"))
        self.assertEqual(["a-;d", "b", "c", "d"], rally.csv_to_list("    a-;d    ,b,c   , d"))

    def test_kv_to_map(self):
        self.assertEqual({}, rally.kv_to_map([]))
        self.assertEqual({"k": "v"}, rally.kv_to_map(["k:'v'"]))
        self.assertEqual({"k": "v", "size": 4, "empty": False, "temperature": 0.5},
                         rally.kv_to_map(["k:'v'", "size:4", "empty:false", "temperature:0.5"]))
