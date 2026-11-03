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

import dataclasses
import hashlib
import json
import logging
import math
import re

from esrally import exceptions
from esrally.utils import console, opts

DEFAULT_MAX_RESPONSE_BYTES = 8 * 1024 * 1024
# A profile is stored in one metrics document, and the bulk helper sends a single document larger than its
# chunk limit unsplit. This bound leaves room for document metadata below EsClient.BULK_MAX_CHUNK_BYTES (50mb).
MAX_RESPONSE_BYTES_LIMIT = 32 * 1024 * 1024

# operation types whose runners support profile capture
SUPPORTED_OPERATION_TYPES = frozenset(["search", "esql"])

# sample-type values of profiling documents; Rally's own summary statistics filter on
# SampleType.Normal (and Warmup for the time window) and never match these
PROFILE_WARMUP_SAMPLE_TYPE = "profile_warmup"
PROFILE_POST_MEASUREMENT_SAMPLE_TYPE = "profile_post_measurement"

ITERATION_RANGE_PATTERN = re.compile(r"^(\d+)-(\d+)$")

LOG = logging.getLogger(__name__)


class CanonicalizationError(exceptions.RallyError):
    """
    Raised when a logical request cannot be canonicalized for fingerprinting, for example because
    it contains values outside the supported JSON types. Fingerprint unavailability is recorded
    explicitly by callers; it never fails profile capture or the benchmark.
    """


def _validate_canonicalizable(value, path):
    if value is None or isinstance(value, (str, bool)):
        return
    if isinstance(value, int):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CanonicalizationError(f"Cannot fingerprint request: non-finite number at [{path}].")
        return
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str):
                raise CanonicalizationError(f"Cannot fingerprint request: non-string key [{key!r}] at [{path}].")
            _validate_canonicalizable(child, f"{path}.{key}")
        return
    if isinstance(value, list):
        for index, child in enumerate(value):
            _validate_canonicalizable(child, f"{path}[{index}]")
        return
    raise CanonicalizationError(f"Cannot fingerprint request: unsupported type [{type(value).__name__}] at [{path}].")


def request_fingerprint(request):
    """
    Computes a deterministic SHA-256 fingerprint (64 lowercase hex chars) of a logical
    Elasticsearch request. Callers must pass the request before profiling-specific parameters
    are injected so that profiled and unprofiled instances of the same logical request
    fingerprint identically.

    Canonicalization accepts only JSON types (objects with string keys, arrays, strings,
    booleans, finite numbers, null), sorts object keys, escapes non-ASCII characters and uses
    compact separators, so the digest is stable across processes and platforms.

    :param request: The logical request as a ``dict``, e.g. operation type, target index,
        final request body and request parameters.
    :return: The hex digest.
    :raises CanonicalizationError: if the request contains values that cannot be canonicalized.
    """
    _validate_canonicalizable(request, path="request")
    try:
        canonical = json.dumps(request, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    except (TypeError, ValueError) as e:
        raise CanonicalizationError(f"Cannot fingerprint request: {e}") from e
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _ensure_bool(value, key, source):
    if not isinstance(value, bool):
        raise exceptions.InvalidSyntax(f"{source}: '{key}' must be a boolean but was [{value!r}].")
    return value


def _ensure_int(value, key, source, minimum, maximum=None):
    # bool is a subclass of int and must not pass as an integer value
    if isinstance(value, bool) or not isinstance(value, int):
        raise exceptions.InvalidSyntax(f"{source}: '{key}' must be an integer but was [{value!r}].")
    if value < minimum:
        raise exceptions.InvalidSyntax(f"{source}: '{key}' must be at least {minimum} but was [{value}].")
    if maximum is not None and value > maximum:
        raise exceptions.InvalidSyntax(f"{source}: '{key}' must be at most {maximum} but was [{value}].")
    return value


def _warn_unknown_keys(raw, known_keys, source):
    unknown = sorted(set(raw) - known_keys)
    if unknown:
        msg = f"{source}: ignoring unknown key(s) {unknown}. Supported keys are {sorted(known_keys)}."
        console.warn(msg, logger=LOG)


class IterationSelector:
    """
    Decides which 1-based, per-client iteration indices are selected for profiling.

    Built from one of:

    * ``"all"``: every iteration is selected.
    * a non-empty list of positive integers, e.g. ``[1, 4, 9]``.
    * an inclusive range string ``"a-b"`` with ``1 <= a <= b``, e.g. ``"1-20"``.
    """

    def __init__(self, iterations=None):
        self._iterations = frozenset(iterations) if iterations is not None else None

    @classmethod
    def from_spec(cls, spec, source):
        if spec == "all":
            return cls()
        if isinstance(spec, str):
            match = ITERATION_RANGE_PATTERN.match(spec.strip())
            if match:
                start, end = int(match.group(1)), int(match.group(2))
                if start < 1:
                    raise exceptions.InvalidSyntax(f"{source}: iteration range [{spec}] must start at 1 or higher.")
                if start > end:
                    raise exceptions.InvalidSyntax(f"{source}: iteration range [{spec}] must satisfy start <= end.")
                return cls(range(start, end + 1))
            raise exceptions.InvalidSyntax(
                f"{source}: 'warmup.iterations' must be 'all', a list of positive integers or a range 'a-b' but was [{spec!r}]."
            )
        if isinstance(spec, list):
            if not spec:
                raise exceptions.InvalidSyntax(
                    f"{source}: 'warmup.iterations' must not be an empty list. Use 'warmup.enabled: false' to disable warmup profiling."
                )
            return cls(_ensure_int(value, "warmup.iterations", source, minimum=1) for value in spec)
        raise exceptions.InvalidSyntax(
            f"{source}: 'warmup.iterations' must be 'all', a list of positive integers or a range 'a-b' but was [{spec!r}]."
        )

    @property
    def all(self):
        return self._iterations is None

    def selects(self, iteration):
        return self._iterations is None or iteration in self._iterations

    def unreached(self, executed_iterations):
        """
        :return: The sorted selected iterations beyond ``executed_iterations``, e.g. because a time-based
            warmup period ended before them. Empty for ``"all"``.
        """
        if self._iterations is None:
            return []
        return sorted(i for i in self._iterations if i > executed_iterations)

    def to_spec(self):
        return "all" if self._iterations is None else sorted(self._iterations)

    def __eq__(self, other):
        return isinstance(other, type(self)) and self._iterations == other._iterations

    def __hash__(self):
        return hash(self._iterations)

    def __repr__(self):
        return f"IterationSelector({self.to_spec()!r})"


@dataclasses.dataclass(frozen=True)
class WarmupProfile:
    enabled: bool = True
    iterations: IterationSelector = dataclasses.field(default_factory=IterationSelector)

    @classmethod
    def from_dict(cls, raw, source):
        if raw is None:
            return cls()
        if not isinstance(raw, dict):
            raise exceptions.InvalidSyntax(f"{source}: 'warmup' must be an object but was [{raw!r}].")
        _warn_unknown_keys(raw, {"enabled", "iterations"}, f"{source}: 'warmup'")
        return cls(
            enabled=_ensure_bool(raw.get("enabled", True), "warmup.enabled", source),
            iterations=IterationSelector.from_spec(raw.get("iterations", "all"), source),
        )

    def to_dict(self):
        return {"enabled": self.enabled, "iterations": self.iterations.to_spec()}


@dataclasses.dataclass(frozen=True)
class PostMeasurementProfile:
    extra_iterations: int = 0

    @classmethod
    def from_dict(cls, raw, source):
        if raw is None:
            return cls()
        if not isinstance(raw, dict):
            raise exceptions.InvalidSyntax(f"{source}: 'post_measurement' must be an object but was [{raw!r}].")
        _warn_unknown_keys(raw, {"extra_iterations"}, f"{source}: 'post_measurement'")
        return cls(extra_iterations=_ensure_int(raw.get("extra_iterations", 0), "post_measurement.extra_iterations", source, minimum=0))

    def to_dict(self):
        return {"extra_iterations": self.extra_iterations}


@dataclasses.dataclass(frozen=True)
class ProfileConfig:
    """
    Resolved profiling configuration. The global configuration comes from ``--profile``;
    a task-level ``profile`` property produces a per-task configuration via ``for_task``.
    Profiling is disabled unless explicitly enabled.
    """

    enabled: bool = False
    warmup: WarmupProfile = dataclasses.field(default_factory=WarmupProfile)
    post_measurement: PostMeasurementProfile = dataclasses.field(default_factory=PostMeasurementProfile)
    max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES

    @classmethod
    def from_dict(cls, raw, source="profile configuration"):
        if raw is None:
            return cls()
        if not isinstance(raw, dict):
            raise exceptions.InvalidSyntax(f"{source}: expected an object but got [{raw!r}].")
        # tolerate the configuration being wrapped in a single 'profile' key
        if set(raw) == {"profile"}:
            return cls.from_dict(raw["profile"], source)
        _warn_unknown_keys(raw, {"enabled", "warmup", "post_measurement", "max_response_bytes"}, source)
        return cls(
            enabled=_ensure_bool(raw.get("enabled", False), "enabled", source),
            warmup=WarmupProfile.from_dict(raw.get("warmup"), source),
            post_measurement=PostMeasurementProfile.from_dict(raw.get("post_measurement"), source),
            max_response_bytes=_ensure_int(
                raw.get("max_response_bytes", DEFAULT_MAX_RESPONSE_BYTES),
                "max_response_bytes",
                source,
                minimum=1,
                maximum=MAX_RESPONSE_BYTES_LIMIT,
            ),
        )

    def to_dict(self):
        return {
            "enabled": self.enabled,
            "warmup": self.warmup.to_dict(),
            "post_measurement": self.post_measurement.to_dict(),
            "max_response_bytes": self.max_response_bytes,
        }

    def for_task(self, profile_spec, task_name=None):
        """
        Resolves the effective configuration for one task.

        :param profile_spec: The task's ``profile`` property: ``None`` (no override), a boolean,
            or an object that is deep-merged over this configuration. An object enables profiling
            for the task unless it sets ``enabled: false``.
        :param task_name: The task name, used in error messages. Optional.
        :return: The task's effective ``ProfileConfig``.
        """
        source = f"task '{task_name}'" if task_name else "task profile override"
        if profile_spec is None:
            return self
        if isinstance(profile_spec, bool):
            return dataclasses.replace(self, enabled=profile_spec)
        if isinstance(profile_spec, dict):
            merged = _deep_merge(self.to_dict(), profile_spec)
            merged["enabled"] = profile_spec.get("enabled", True)
            return ProfileConfig.from_dict(merged, source)
        raise exceptions.InvalidSyntax(f"{source}: 'profile' must be a boolean or an object but was [{profile_spec!r}].")


_KNOWN_KEYS = {
    "enabled": None,
    "warmup": {"enabled", "iterations"},
    "post_measurement": {"extra_iterations"},
    "max_response_bytes": None,
}


def known_keys_only(profile_spec):
    """
    Returns a task-level ``profile`` property without unknown keys. The loader warns about unknown keys
    once and stores the sanitized value, so resolving it again on load driver workers stays silent.
    """
    if not isinstance(profile_spec, dict):
        return profile_spec
    sanitized = {}
    for key, value in profile_spec.items():
        if key not in _KNOWN_KEYS:
            continue
        nested_keys = _KNOWN_KEYS[key]
        if nested_keys is not None and isinstance(value, dict):
            value = {k: v for k, v in value.items() if k in nested_keys}
        sanitized[key] = value
    return sanitized


def effective_task_config(global_config, profile_spec, task_name, operation_type):
    """
    Resolves the profiling configuration that applies to one task at runtime.

    :param global_config: The global ``ProfileConfig`` or ``None`` if none is configured.
    :param profile_spec: The task's ``profile`` property (``None``, a boolean or an object).
    :param task_name: The task name.
    :param operation_type: The task's operation type.
    :return: The task's ``ProfileConfig`` if profiling applies to this task, otherwise ``None``.
        A globally enabled configuration applies only to supported operation types.
    """
    if global_config is None:
        global_config = ProfileConfig()
    if profile_spec is None and not global_config.enabled:
        return None
    task_config = global_config.for_task(profile_spec, task_name)
    if not task_config.enabled or operation_type not in SUPPORTED_OPERATION_TYPES:
        return None
    return task_config


def _deep_merge(base, override):
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def from_cli(arg):
    """
    Parses the ``--profile`` command line value: inline JSON, a path to a JSON file,
    or key-value pairs for flat settings (e.g. ``enabled:true``). ``None`` keeps profiling disabled.
    """
    if arg is None:
        return ProfileConfig()
    try:
        raw = opts.to_dict(arg)
    except exceptions.RallyError:
        raise
    except Exception as e:
        raise exceptions.InvalidSyntax(
            f"--profile must be inline JSON, a path to a JSON file or key-value pairs but was [{arg}]: {e}"
        ) from e
    return ProfileConfig.from_dict(raw, source="--profile")
