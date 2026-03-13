import re
import warnings
from collections.abc import Mapping
from datetime import date, datetime
from typing import Any

import elastic_transport
import elasticsearch
from elastic_transport.client_utils import percent_encode

from esrally.utils import versions
from esrally.version import minimum_es_version

_WARNING_RE = re.compile(r"\"([^\"]*)\"")
_COMPAT_MIMETYPE_RE = re.compile(r"application/(json|x-ndjson|vnd\.mapbox-vector-tile)")

_MIN_COMPATIBILITY_MODE = int(versions.Version.from_string(minimum_es_version()).major)
_MAX_COMPATIBILITY_MODE = int(elasticsearch.VERSION[0])
_VALID_COMPATIBILITY_MODES = tuple(range(_MIN_COMPATIBILITY_MODE, _MAX_COMPATIBILITY_MODE + 1))
assert len(_VALID_COMPATIBILITY_MODES) > 0, "There should be at least one valid compatibility mode."


def ensure_mimetype_headers(
    *,
    headers: Mapping[str, str] | None = None,
    path: str | None = None,
    body: str | None = None,
    version: str | int | None = None,
) -> elastic_transport.HttpHeaders:
    # Ensure will use a case-insensitive copy of input headers.
    headers = elastic_transport.HttpHeaders(headers or {})

    if body is not None:
        # Newer Elasticsearch versions are picky about the content-type header when a body is present.
        # Because tracks are not passing headers explicitly, set them here.
        mimetype = "application/json"
        if path and path.endswith("/_bulk"):
            # Server version 9 is picky about this. This should improve compatibility.
            mimetype = "application/x-ndjson"
        for header in ("content-type", "accept"):
            headers.setdefault(header, mimetype)

    # Ensures compatibility mode is being applied to mime type.
    # If not doing, the vanilla client version 9 will by default ask for compatibility mode 9, which would not
    # allow connecting to server version 8 clusters.
    try:
        compatibility_mode = get_compatibility_mode(version=version)
    except ValueError as ex:
        warnings.warn(f"Invalid compatibility mode {version!r}, defaulting to {_MIN_COMPATIBILITY_MODE!r}: {ex}")
        compatibility_mode = _MIN_COMPATIBILITY_MODE
    for header in ("accept", "content-type"):
        mimetype = headers.get(header)
        if mimetype is None:
            continue
        headers[header] = _COMPAT_MIMETYPE_RE.sub(
            "application/vnd.elasticsearch+%s; compatible-with=%s" % (r"\g<1>", compatibility_mode), mimetype
        )
    return headers


def get_compatibility_mode(version: str | int | None = None) -> int:
    if version is None:
        # By default, return the minimum compatibility mode for better compatibility.
        return _MIN_COMPATIBILITY_MODE

    # Normalize version to an integer major version.
    if isinstance(version, str):
        if not versions.is_version_identifier(version):
            raise ValueError(f"Elasticsearch version {version!r} is not valid.")
        version = int(versions.Version.from_string(version).major)

    if not isinstance(version, int):
        raise TypeError(f"Version must be a valid version string or an integer, but got {version!r}.")

    if version not in _VALID_COMPATIBILITY_MODES:
        supported = ", ".join(str(v) for v in _VALID_COMPATIBILITY_MODES)
        raise ValueError(f"Elasticsearch version {version!r} is not supported, supported versions are: {supported}.")
    return version


def _escape(value: Any) -> str:
    """
    Escape a single value of a URL string or a query parameter. If it is a list
    or tuple, turn it into a comma-separated string first.
    """

    # make sequences into comma-separated stings
    if isinstance(value, (list, tuple)):
        value = ",".join([_escape(item) for item in value])

    # dates and datetimes into isoformat
    elif isinstance(value, (date, datetime)):
        value = value.isoformat()

    # make bools into true/false strings
    elif isinstance(value, bool):
        value = str(value).lower()

    elif isinstance(value, bytes):
        return value.decode("utf-8", "surrogatepass")

    if not isinstance(value, str):
        return str(value)
    return value


def _quote(value: Any) -> str:
    return percent_encode(_escape(value), ",*")


def _quote_query(query: Mapping[str, Any]) -> str:
    return "&".join([f"{k}={_quote(v)}" for k, v in query.items()])
