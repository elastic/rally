# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import datetime
import json
import pickle
import subprocess
import sys

import pytest

from esrally import exceptions, profiling
from esrally.profiling import (
    CanonicalizationError,
    IterationSelector,
    ProfileConfig,
    request_fingerprint,
)


class TestIterationSelector:
    def test_all_selects_every_iteration(self):
        selector = IterationSelector.from_spec("all", source="test")
        assert selector.all
        for iteration in [1, 2, 100, 10_000]:
            assert selector.selects(iteration)
        assert selector.to_spec() == "all"

    def test_list_selects_exactly_the_given_iterations(self):
        selector = IterationSelector.from_spec([9, 1, 4], source="test")
        assert not selector.all
        assert [i for i in range(1, 11) if selector.selects(i)] == [1, 4, 9]
        assert selector.to_spec() == [1, 4, 9]

    def test_range_is_inclusive(self):
        selector = IterationSelector.from_spec("3-5", source="test")
        assert [i for i in range(1, 8) if selector.selects(i)] == [3, 4, 5]

    def test_single_iteration_range(self):
        selector = IterationSelector.from_spec("1-1", source="test")
        assert selector.selects(1)
        assert not selector.selects(2)

    def test_duplicate_list_entries_are_deduplicated(self):
        selector = IterationSelector.from_spec([2, 2, 3], source="test")
        assert selector.to_spec() == [2, 3]

    @pytest.mark.parametrize(
        "spec",
        [
            [],
            [0],
            [-1],
            [1.5],
            [True],
            ["1"],
            "9-4",
            "0-5",
            "a-b",
            "none",
            42,
            {"from": 1},
        ],
    )
    def test_invalid_specs_are_rejected(self, spec):
        with pytest.raises(exceptions.InvalidSyntax):
            IterationSelector.from_spec(spec, source="test")

    def test_equality(self):
        assert IterationSelector.from_spec("1-3", source="test") == IterationSelector.from_spec([1, 2, 3], source="test")
        assert IterationSelector.from_spec("all", source="test") != IterationSelector.from_spec([1], source="test")


class TestProfileConfig:
    def test_disabled_by_default(self):
        cfg = ProfileConfig.from_dict(None)
        assert not cfg.enabled
        assert cfg.warmup.enabled
        assert cfg.warmup.iterations.all
        assert cfg.post_measurement.extra_iterations == 0
        assert cfg.max_response_bytes == profiling.DEFAULT_MAX_RESPONSE_BYTES

    def test_enabled_with_defaults(self):
        cfg = ProfileConfig.from_dict({"enabled": True})
        assert cfg.enabled
        assert cfg.warmup.enabled
        assert cfg.warmup.iterations.all
        assert cfg.post_measurement.extra_iterations == 0

    def test_full_configuration(self):
        cfg = ProfileConfig.from_dict(
            {
                "enabled": True,
                "warmup": {"enabled": True, "iterations": [1, 4, 9]},
                "post_measurement": {"extra_iterations": 3},
                "max_response_bytes": 1024,
            }
        )
        assert cfg.enabled
        assert cfg.warmup.iterations.to_spec() == [1, 4, 9]
        assert cfg.post_measurement.extra_iterations == 3
        assert cfg.max_response_bytes == 1024

    def test_accepts_profile_wrapper(self):
        cfg = ProfileConfig.from_dict({"profile": {"enabled": True, "warmup": {"iterations": "1-20"}}})
        assert cfg.enabled
        assert cfg.warmup.iterations.selects(20)
        assert not cfg.warmup.iterations.selects(21)

    def test_unknown_keys_warn_and_are_ignored(self, caplog):
        with caplog.at_level("WARNING", logger="esrally.profiling"):
            cfg = ProfileConfig.from_dict(
                {
                    "enabled": True,
                    "sampling": 0.5,
                    "warmup": {"iterations": "all", "cadence": 2},
                    "post_measurement": {"extra_iterations": 1, "mode": "fast"},
                }
            )
        assert cfg.enabled
        assert cfg.post_measurement.extra_iterations == 1
        warnings = [r.message for r in caplog.records]
        assert any("['sampling']" in w for w in warnings)
        assert any("['cadence']" in w for w in warnings)
        assert any("['mode']" in w for w in warnings)

    @pytest.mark.parametrize(
        "raw",
        [
            "enabled",
            {"enabled": "yes"},
            {"enabled": 1},
            {"warmup": "all"},
            {"warmup": {"enabled": "no"}},
            {"post_measurement": 3},
            {"post_measurement": {"extra_iterations": -1}},
            {"post_measurement": {"extra_iterations": True}},
            {"post_measurement": {"extra_iterations": "3"}},
            {"max_response_bytes": 0},
            {"max_response_bytes": "8MB"},
            {"max_response_bytes": profiling.MAX_RESPONSE_BYTES_LIMIT + 1},
        ],
    )
    def test_invalid_values_are_rejected(self, raw):
        with pytest.raises(exceptions.InvalidSyntax):
            ProfileConfig.from_dict(raw)

    def test_round_trips_through_to_dict(self):
        raw = {
            "enabled": True,
            "warmup": {"enabled": False, "iterations": [2, 7]},
            "post_measurement": {"extra_iterations": 5},
            "max_response_bytes": 2048,
        }
        cfg = ProfileConfig.from_dict(raw)
        assert ProfileConfig.from_dict(cfg.to_dict()) == cfg

    def test_is_picklable(self):
        cfg = ProfileConfig.from_dict({"enabled": True, "warmup": {"iterations": [1, 4]}})
        assert pickle.loads(pickle.dumps(cfg)) == cfg


class TestForTask:
    def global_config(self, enabled=True):
        return ProfileConfig.from_dict({"enabled": enabled, "warmup": {"iterations": "1-20"}})

    def test_no_override_returns_global_config(self):
        cfg = self.global_config()
        assert cfg.for_task(None) is cfg

    def test_boolean_false_disables_task(self):
        task_cfg = self.global_config().for_task(False, task_name="term-query")
        assert not task_cfg.enabled
        # non-enabled settings are retained from the global configuration
        assert task_cfg.warmup.iterations.selects(20)

    def test_boolean_true_enables_task_with_global_settings(self):
        task_cfg = self.global_config(enabled=False).for_task(True, task_name="term-query")
        assert task_cfg.enabled
        assert task_cfg.warmup.iterations.selects(20)
        assert not task_cfg.warmup.iterations.selects(21)

    def test_object_override_deep_merges_over_global(self):
        task_cfg = self.global_config().for_task({"warmup": {"iterations": [1, 4, 9]}}, task_name="term-query")
        assert task_cfg.enabled
        assert task_cfg.warmup.enabled
        assert task_cfg.warmup.iterations.to_spec() == [1, 4, 9]
        assert task_cfg.post_measurement.extra_iterations == 0

    def test_object_override_enables_task_even_when_global_is_disabled(self):
        task_cfg = self.global_config(enabled=False).for_task({"warmup": {"iterations": [1]}}, task_name="term-query")
        assert task_cfg.enabled

    def test_object_override_respects_explicit_enabled_false(self):
        task_cfg = self.global_config().for_task({"enabled": False, "warmup": {"iterations": [1]}}, task_name="term-query")
        assert not task_cfg.enabled

    def test_max_response_bytes_accepts_the_limit(self):
        cfg = ProfileConfig.from_dict({"max_response_bytes": profiling.MAX_RESPONSE_BYTES_LIMIT})
        assert cfg.max_response_bytes == profiling.MAX_RESPONSE_BYTES_LIMIT

    def test_task_override_cannot_exceed_the_limit(self):
        with pytest.raises(exceptions.InvalidSyntax, match="at most"):
            self.global_config().for_task({"max_response_bytes": profiling.MAX_RESPONSE_BYTES_LIMIT + 1}, task_name="term-query")

    def test_object_override_of_max_response_bytes(self):
        task_cfg = self.global_config().for_task({"max_response_bytes": 512}, task_name="term-query")
        assert task_cfg.max_response_bytes == 512

    def test_object_override_of_post_measurement(self):
        task_cfg = self.global_config().for_task({"post_measurement": {"extra_iterations": 7}}, task_name="term-query")
        assert task_cfg.post_measurement.extra_iterations == 7

    def test_unknown_task_keys_warn_with_task_source(self, caplog):
        with caplog.at_level("WARNING", logger="esrally.profiling"):
            self.global_config().for_task({"iterations": [1]}, task_name="term-query")
        assert any("task 'term-query'" in r.message and "['iterations']" in r.message for r in caplog.records)

    @pytest.mark.parametrize("spec", ["true", 1, [1, 2]])
    def test_invalid_override_types_are_rejected(self, spec):
        with pytest.raises(exceptions.InvalidSyntax):
            self.global_config().for_task(spec, task_name="term-query")

    def test_invalid_override_error_names_the_task(self):
        with pytest.raises(exceptions.InvalidSyntax, match="task 'term-query'"):
            self.global_config().for_task({"warmup": {"iterations": []}}, task_name="term-query")


class TestFromCli:
    def test_none_keeps_profiling_disabled(self):
        assert profiling.from_cli(None) == ProfileConfig()

    def test_inline_json(self):
        cfg = profiling.from_cli('{"enabled": true, "post_measurement": {"extra_iterations": 2}}')
        assert cfg.enabled
        assert cfg.post_measurement.extra_iterations == 2

    def test_json_file(self, tmp_path):
        config_file = tmp_path / "profile.json"
        config_file.write_text(json.dumps({"enabled": True, "warmup": {"iterations": "1-5"}}), encoding="utf-8")
        cfg = profiling.from_cli(str(config_file))
        assert cfg.enabled
        assert cfg.warmup.iterations.selects(5)
        assert not cfg.warmup.iterations.selects(6)

    def test_key_value_pairs(self):
        cfg = profiling.from_cli("enabled:true")
        assert cfg.enabled

    def test_invalid_value_is_rejected(self):
        with pytest.raises(exceptions.InvalidSyntax, match="--profile"):
            profiling.from_cli("not json at all")

    def test_invalid_nested_value_is_rejected(self):
        with pytest.raises(exceptions.InvalidSyntax, match="--profile"):
            profiling.from_cli('{"enabled": true, "post_measurement": {"extra_iterations": -3}}')


class TestRequestFingerprint:
    def test_is_full_sha256_hex(self):
        digest = request_fingerprint({"operation-type": "search", "index": "logs", "body": {"query": {"match_all": {}}}})
        assert len(digest) == 64
        assert all(c in "0123456789abcdef" for c in digest)

    def test_key_order_does_not_matter(self):
        a = request_fingerprint({"body": {"size": 10, "query": {"term": {"f": "v"}}}, "index": "logs"})
        b = request_fingerprint({"index": "logs", "body": {"query": {"term": {"f": "v"}}, "size": 10}})
        assert a == b

    def test_nested_objects_and_lists(self):
        a = request_fingerprint({"body": {"bool": {"must": [{"term": {"a": 1}}, {"term": {"b": 2}}]}}})
        b = request_fingerprint({"body": {"bool": {"must": [{"term": {"a": 1}}, {"term": {"b": 2}}]}}})
        assert a == b

    def test_list_order_is_significant(self):
        a = request_fingerprint({"body": {"must": [{"a": 1}, {"b": 2}]}})
        b = request_fingerprint({"body": {"must": [{"b": 2}, {"a": 1}]}})
        assert a != b

    def test_value_differences_change_the_digest(self):
        base = request_fingerprint({"body": {"size": 10}})
        assert request_fingerprint({"body": {"size": 11}}) != base

    def test_numeric_types_are_distinguished(self):
        # 1 and 1.0 serialize differently in JSON and are distinct requests
        assert request_fingerprint({"v": 1}) != request_fingerprint({"v": 1.0})
        # bool is not a number
        assert request_fingerprint({"v": True}) != request_fingerprint({"v": 1})

    def test_unicode_is_deterministic(self):
        a = request_fingerprint({"q": "café 日本"})
        b = request_fingerprint({"q": "café 日本"})
        assert a == b

    def test_profiling_flag_exclusion_is_callers_responsibility(self):
        # the same logical request with and without the profile flag differs by construction;
        # callers therefore fingerprint before injecting it
        without_flag = request_fingerprint({"body": {"query": {"match_all": {}}}})
        with_flag = request_fingerprint({"body": {"query": {"match_all": {}}, "profile": True}})
        assert with_flag != without_flag

    @pytest.mark.parametrize(
        "request_dict",
        [
            {"v": float("nan")},
            {"v": float("inf")},
            {"v": datetime.datetime(2026, 1, 1)},
            {"v": {1: "non-string-key"}},
            {"v": {"nested": [{"deep": b"bytes"}]}},
            {"v": ("tuple", "unsupported")},
        ],
    )
    def test_unsupported_values_raise_canonicalization_error(self, request_dict):
        with pytest.raises(CanonicalizationError):
            request_fingerprint(request_dict)

    def test_error_names_the_path(self):
        with pytest.raises(CanonicalizationError, match=r"request\.body\.pit\[0\]"):
            request_fingerprint({"body": {"pit": [object()]}})

    def test_stable_across_processes(self):
        request = {"operation-type": "search", "index": "logs", "body": {"query": {"term": {"f": "café"}}, "size": 7}}
        in_process = request_fingerprint(request)
        script = (
            "import json,sys\n"
            "from esrally.profiling import request_fingerprint\n"
            "print(request_fingerprint(json.loads(sys.argv[1])))\n"
        )
        out = subprocess.run(
            [sys.executable, "-c", script, json.dumps(request)],
            capture_output=True,
            text=True,
            check=True,
        )
        assert out.stdout.strip() == in_process


class TestRuntimeResolution:
    def test_unreached_iterations(self):
        assert IterationSelector.from_spec([2, 7, 9], source="test").unreached(7) == [9]
        assert IterationSelector.from_spec([2, 7, 9], source="test").unreached(10) == []
        assert IterationSelector.from_spec("all", source="test").unreached(0) == []

    def test_known_keys_only_drops_unknown_keys(self):
        spec = {"enabled": True, "sampling": 1, "warmup": {"iterations": [1], "cadence": 2}, "post_measurement": {"x": 1}}
        assert profiling.known_keys_only(spec) == {"enabled": True, "warmup": {"iterations": [1]}, "post_measurement": {}}
        assert profiling.known_keys_only(True) is True

    def test_no_configuration_disables_profiling(self):
        assert profiling.effective_task_config(None, None, "t", "search") is None
        assert profiling.effective_task_config(ProfileConfig(), None, "t", "search") is None

    def test_global_configuration_applies_to_supported_operation_types_only(self):
        global_config = ProfileConfig.from_dict({"enabled": True})
        assert profiling.effective_task_config(global_config, None, "t", "search") == global_config
        assert profiling.effective_task_config(global_config, None, "t", "esql") == global_config
        assert profiling.effective_task_config(global_config, None, "t", "bulk") is None
        assert profiling.effective_task_config(global_config, None, "t", "paginated-search") is None

    def test_task_override_enables_without_global_configuration(self):
        task_config = profiling.effective_task_config(None, {"warmup": {"iterations": [3]}}, "t", "esql")
        assert task_config.enabled
        assert task_config.warmup.iterations.to_spec() == [3]

    def test_task_override_disables_global_configuration(self):
        global_config = ProfileConfig.from_dict({"enabled": True})
        assert profiling.effective_task_config(global_config, False, "t", "search") is None
