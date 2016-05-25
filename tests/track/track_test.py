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


class TrackReaderTests(TestCase):
    def test_missing_description_raises_syntax_error(self):
        track_specification = {
            "meta": {
                "name": "unittest"
            }
        }
        reader = track.TrackReader()
        with self.assertRaises(track.TrackSyntaxError) as ctx:
            reader.read(track_specification)
        self.assertEqual("Mandatory element 'meta.short-description' is missing.", ctx.exception.args[0])

    def test_wrong_type_for_name_raises_syntax_error(self):
        track_specification = {
            "meta": {
                "name": 1.03
            }
        }
        reader = track.TrackReader()
        with self.assertRaises(track.TrackSyntaxError) as ctx:
            reader.read(track_specification)
        self.assertEqual("Value '1.03' of element 'meta.name' is not of expected type '<class 'str'>'", ctx.exception.args[0])

    def test_parse_valid_track_specification(self):
        track_specification = {
            "meta": {
                "name": "unittest",
                "short-description": "short description for unit test",
                "description": "longer description of this track for unit test",
                "data-url": "https://localhost/data"
            },
            "indices": [
                {
                    "name": "index-historical",
                    "types": [
                        {
                            "name": "main",
                            "documents": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                            "mapping": "main-type-mappings.json"
                        },
                        {
                            "name": "secondary",
                            "documents": "documents-secondary.json.bz2",
                            "document-count": 20,
                            "compressed-bytes": 200,
                            "uncompressed-bytes": 20000,
                            "mapping": "secondary-type-mappings.json"
                        }

                    ]
                }
            ],
            "operations": [
                {
                    "index-append": {
                        "type": "index",
                        "index-settings": {},
                        "clients": {
                            "count": 8
                        },
                        "bulk-size": 5000,
                        "force-merge": False
                    }
                }
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "description": "Default challenge",
                    "schedule": [
                        "index-append"
                    ]
                }

            ]
        }
        reader = track.TrackReader()
        resulting_track = reader.read(track_specification)
        self.assertEqual("unittest", resulting_track.name)
        self.assertEqual("short description for unit test", resulting_track.short_description)
        self.assertEqual("longer description of this track for unit test", resulting_track.description)
        self.assertEqual(1, len(resulting_track.indices))
        self.assertEqual("index-historical", resulting_track.indices[0].name)
        self.assertEqual(2, len(resulting_track.indices[0].types))
        self.assertEqual("main", resulting_track.indices[0].types[0].name)
        self.assertEqual("secondary", resulting_track.indices[0].types[1].name)
        self.assertEqual(1, len(resulting_track.challenges))
        self.assertEqual("default-challenge", resulting_track.challenges[0].name)

