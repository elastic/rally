from unittest import TestCase

import jinja2

from esrally.track import loader


class StaticClock:
    NOW = 1453362707.0

    @staticmethod
    def now():
        return StaticClock.NOW

    @staticmethod
    def stop_watch():
        return None


class TemplateRenderTests(TestCase):
    def test_render_template(self):
        template = """
        {
            "key": {{'01-01-2000' | days_ago(now)}},
            "key2": "static value"
        }
        """

        rendered = loader.render_template(loader=jinja2.DictLoader({"unittest": template}), template_name="unittest", clock=StaticClock)

        expected = """
        {
            "key": 5864,
            "key2": "static value"
        }
        """
        self.assertEqual(expected, rendered)


class TrackSpecificationReaderTests(TestCase):
    def test_missing_description_raises_syntax_error(self):
        track_specification = {
            "description": "unittest track"
        }
        reader = loader.TrackSpecificationReader()
        with self.assertRaises(loader.TrackSyntaxError) as ctx:
            reader("unittest", track_specification, "/mappings", "/data")
        self.assertEqual("Track 'unittest' is invalid. Mandatory element 'short-description' is missing.", ctx.exception.args[0])

    def test_can_read_track_info_from_meta_block(self):
        # This test checks for the old syntax where track info was contained in a meta-block
        track_specification = {
            "meta": {
                "short-description": "short description for unit test",
                "description": "longer description of this track for unit test",
                "data-url": "https://localhost/data"
            },
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [],
            "challenges": []
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings", "/data")
        self.assertEqual("unittest", resulting_track.name)
        self.assertEqual("short description for unit test", resulting_track.short_description)
        self.assertEqual("longer description of this track for unit test", resulting_track.description)
        self.assertEqual("https://localhost/data", resulting_track.source_root_url)

    def test_parse_with_mixed_warmup_iterations_and_measurement(self):
        track_specification = {
            "short-description": "short description for unit test",
            "description": "longer description of this track for unit test",
            "data-url": "https://localhost/data",
            "indices": [
                {
                    "name": "test-index",
                    "types": [
                        {
                            "name": "main",
                            "documents": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                            "mapping": "main-type-mappings.json"
                        }
                    ]
                }
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index",
                    "bulk-size": 5000,
                }
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "description": "Default challenge",
                    "schedule": [
                        {
                            "index-settings": {},
                            "clients": 8,
                            "operation": "index-append",
                            "warmup-iterations": 3,
                            "time-period": 60
                        }
                    ]
                }

            ]
        }

        reader = loader.TrackSpecificationReader()
        with self.assertRaises(loader.TrackSyntaxError) as ctx:
            reader("unittest", track_specification, "/mappings", "/data")
        self.assertEqual("Track 'unittest' is invalid. Operation 'index-append' in challenge 'default-challenge' defines '3' warmup "
                         "iterations and a time period of '60' seconds. Please do not mix time periods and iterations.",
                         ctx.exception.args[0])

    def test_parse_with_mixed_warmup_timeperiod_and_iterations(self):
        track_specification = {
            "short-description": "short description for unit test",
            "description": "longer description of this track for unit test",
            "data-url": "https://localhost/data",
            "indices": [
                {
                    "name": "test-index",
                    "types": [
                        {
                            "name": "main",
                            "documents": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                            "mapping": "main-type-mappings.json"
                        }
                    ]
                }
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index",
                    "bulk-size": 5000,
                }
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "description": "Default challenge",
                    "schedule": [
                        {
                            "index-settings": {},
                            "clients": 8,
                            "operation": "index-append",
                            "warmup-time-period": 20,
                            "iterations": 1000
                        }
                    ]
                }

            ]
        }

        reader = loader.TrackSpecificationReader()
        with self.assertRaises(loader.TrackSyntaxError) as ctx:
            reader("unittest", track_specification, "/mappings", "/data")
        self.assertEqual("Track 'unittest' is invalid. Operation 'index-append' in challenge 'default-challenge' defines a warmup time "
                         "period of '20' seconds and '1000' iterations. Please do not mix time periods and iterations.",
                         ctx.exception.args[0])

    def test_parse_valid_track_specification(self):
        track_specification = {
            "short-description": "short description for unit test",
            "description": "longer description of this track for unit test",
            "data-url": "https://localhost/data",
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
                    "name": "index-append",
                    "operation-type": "index",
                    "bulk-size": 5000,
                    "meta": {
                        "append": True
                    }
                },
                {
                    "name": "search",
                    "operation-type": "search",
                    "index": "index-historical"
                }
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "description": "Default challenge",
                    "meta": {
                        "mixed": True,
                        "max-clients": 8
                    },
                    "schedule": [
                        {
                            "index-settings": {},
                            "clients": 8,
                            "operation": "index-append",
                            "meta": {
                                "operation-index": 0
                            }
                        },
                        {
                            "clients": 1,
                            "operation": "search"
                        }
                    ]
                }

            ]
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings", "/data")
        self.assertEqual("unittest", resulting_track.name)
        self.assertEqual("short description for unit test", resulting_track.short_description)
        self.assertEqual("longer description of this track for unit test", resulting_track.description)
        self.assertEqual(1, len(resulting_track.indices))
        self.assertEqual("index-historical", resulting_track.indices[0].name)
        self.assertEqual(2, len(resulting_track.indices[0].types))
        self.assertEqual("main", resulting_track.indices[0].types[0].name)
        self.assertEqual("/data/documents-main.json.bz2", resulting_track.indices[0].types[0].document_archive)
        self.assertEqual("/data/documents-main.json", resulting_track.indices[0].types[0].document_file)
        self.assertEqual("/mappings/main-type-mappings.json", resulting_track.indices[0].types[0].mapping_file)
        self.assertEqual("secondary", resulting_track.indices[0].types[1].name)
        self.assertEqual(1, len(resulting_track.challenges))
        self.assertEqual("default-challenge", resulting_track.challenges[0].name)
        self.assertEqual({"mixed": True, "max-clients": 8}, resulting_track.challenges[0].meta_data)
        self.assertEqual({"append": True}, resulting_track.challenges[0].schedule[0].operation.meta_data)
        self.assertEqual({"operation-index": 0}, resulting_track.challenges[0].schedule[0].meta_data)

    def test_parse_valid_track_specification_with_index_template(self):
        track_specification = {
            "short-description": "short description for unit test",
            "description": "longer description of this track for unit test",
            "templates": [
                {
                    "name": "my-index-template",
                    "index-pattern": "*",
                    "template": "default-template.json"
                }
            ],
            "operations": [],
            "challenges": []
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings", "/data")
        self.assertEqual("unittest", resulting_track.name)
        self.assertEqual("short description for unit test", resulting_track.short_description)
        self.assertEqual("longer description of this track for unit test", resulting_track.description)
        self.assertEqual(0, len(resulting_track.indices))
        self.assertEqual(1, len(resulting_track.templates))
        self.assertEqual("my-index-template", resulting_track.templates[0].name)
        self.assertEqual("*", resulting_track.templates[0].pattern)
        self.assertEqual("/mappings/default-template.json", resulting_track.templates[0].template_file)
        self.assertEqual(0, len(resulting_track.challenges))

    def test_types_are_optional_for_user_managed_indices(self):
        track_specification = {
            "short-description": "short description for unit test",
            "description": "longer description of this track for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [],
            "challenges": []
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings", "/data")
        self.assertEqual("unittest", resulting_track.name)
        self.assertEqual("short description for unit test", resulting_track.short_description)
        self.assertEqual("longer description of this track for unit test", resulting_track.description)
        self.assertEqual(1, len(resulting_track.indices))
        self.assertEqual(0, len(resulting_track.templates))
        self.assertEqual("test-index", resulting_track.indices[0].name)
        self.assertEqual(0, len(resulting_track.indices[0].types))

    def test_unique_challenge_names(self):
        track_specification = {
            "short-description": "short description for unit test",
            "description": "longer description of this track for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index"
                }
            ],
            "challenges": [
                {
                    "name": "test-challenge",
                    "description": "Some challenge",
                    "default": True,
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                },
                {
                    "name": "test-challenge",
                    "description": "Another challenge with the same name",
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                }

            ]
        }
        reader = loader.TrackSpecificationReader()
        with self.assertRaises(loader.TrackSyntaxError) as ctx:
            reader("unittest", track_specification, "/mappings", "/data")
        self.assertEqual("Track 'unittest' is invalid. Duplicate challenge with name 'test-challenge'.", ctx.exception.args[0])

    def test_not_more_than_one_default_challenge_possible(self):
        track_specification = {
            "short-description": "short description for unit test",
            "description": "longer description of this track for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index"
                }
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "description": "Default challenge",
                    "default": True,
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                },
                {
                    "name": "another-challenge",
                    "description": "See if we can sneek it in as another default",
                    "default": True,
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                }

            ]
        }
        reader = loader.TrackSpecificationReader()
        with self.assertRaises(loader.TrackSyntaxError) as ctx:
            reader("unittest", track_specification, "/mappings", "/data")
        self.assertEqual("Track 'unittest' is invalid. Both 'default-challenge' and 'another-challenge' are defined as default challenges. "
                         "Please define only one of them as default.", ctx.exception.args[0])

    def test_at_least_one_default_challenge(self):
        track_specification = {
            "short-description": "short description for unit test",
            "description": "longer description of this track for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index"
                }
            ],
            "challenges": [
                {
                    "name": "challenge",
                    "description": "Some challenge",
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                },
                {
                    "name": "another-challenge",
                    "description": "Another challenge",
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                }

            ]
        }
        reader = loader.TrackSpecificationReader()
        with self.assertRaises(loader.TrackSyntaxError) as ctx:
            reader("unittest", track_specification, "/mappings", "/data")
        self.assertEqual("Track 'unittest' is invalid. No default challenge specified. Please edit the track and add \"default\": true "
                         "to one of the challenges challenge, another-challenge.", ctx.exception.args[0])

    def test_exactly_one_default_challenge(self):
        track_specification = {
            "short-description": "short description for unit test",
            "description": "longer description of this track for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index"
                }
            ],
            "challenges": [
                {
                    "name": "challenge",
                    "description": "Some challenge",
                    "default": True,
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                },
                {
                    "name": "another-challenge",
                    "description": "Another challenge",
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                }

            ]
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings", "/data")
        self.assertEqual(2, len(resulting_track.challenges))
        self.assertEqual("challenge", resulting_track.challenges[0].name)
        self.assertTrue(resulting_track.challenges[0].default)
        self.assertFalse(resulting_track.challenges[1].default)

    def test_selects_sole_challenge_implicitly_as_default(self):
        track_specification = {
            "short-description": "short description for unit test",
            "description": "longer description of this track for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index"
                }
            ],
            "challenges": [
                {
                    "name": "challenge",
                    "description": "Some challenge",
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                }
            ]
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings", "/data")
        self.assertEqual(1, len(resulting_track.challenges))
        self.assertEqual("challenge", resulting_track.challenges[0].name)
        self.assertTrue(resulting_track.challenges[0].default)

    def test_either_target_throughput_or_target_interval(self):
        track_specification = {
            "short-description": "short description for unit test",
            "description": "longer description of this track for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index"
                }
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "description": "Default challenge",
                    "schedule": [
                        {
                            "operation": "index-append",
                            "target-throughput": 10,
                            "target-interval": 3
                        }
                    ]
                }
            ]
        }
        reader = loader.TrackSpecificationReader()
        with self.assertRaises(loader.TrackSyntaxError) as ctx:
            reader("unittest", track_specification, "/mappings", "/data")
        self.assertEqual("Track 'unittest' is invalid. Operation 'index-append' in challenge 'default-challenge' specifies target-interval "
                         "and target-throughput but only one of them is allowed.", ctx.exception.args[0])

    def test_supports_target_throughput(self):
        track_specification = {
            "short-description": "short description for unit test",
            "description": "longer description of this track for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index"
                }
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "description": "Default challenge",
                    "schedule": [
                        {
                            "operation": "index-append",
                            "target-throughput": 10,
                        }
                    ]
                }
            ]
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings", "/data")
        self.assertEqual(10, resulting_track.challenges[0].schedule[0].target_throughput)

    def test_supports_target_interval(self):
        track_specification = {
            "short-description": "short description for unit test",
            "description": "longer description of this track for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index"
                }
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "description": "Default challenge",
                    "schedule": [
                        {
                            "operation": "index-append",
                            "target-interval": 5,
                        }
                    ]
                }
            ]
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings", "/data")
        self.assertEqual(0.2, resulting_track.challenges[0].schedule[0].target_throughput)
