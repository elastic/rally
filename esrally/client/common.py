import re
from collections.abc import Mapping
from datetime import date, datetime
from typing import Any

import elastic_transport
from elastic_transport.client_utils import percent_encode

from esrally.utils import versions

_WARNING_RE = re.compile(r"\"([^\"]*)\"")
_COMPAT_MIMETYPE_RE = re.compile(r"application/(json|x-ndjson|vnd\.mapbox-vector-tile)")


def compatibility_mode_from_distribution(*, version: str | None = None, flavour: str | None = None) -> int | None:
    if flavour and versions.is_serverless(flavour):
        # Serverless doesn't need compatibility mode.
        return None
    if version and versions.is_version_identifier(version):
        return int(versions.Version.from_string(version).major)

    # To improve compatibilit it prefers to ignore compatibility mode in case it can't understand distribution version.
    return None


def ensure_mimetype_headers(
    *,
    headers: Mapping[str, str] | None = None,
    path: str | None = None,
    body: str | None = None,
    compatibility_mode: int | None = None,
) -> elastic_transport.HttpHeaders:
    # It makes sure it uses a case-insensitive copy of input headers.
    headers = elastic_transport.HttpHeaders(headers or {})

    if body is not None:
        # It ensures content-type and accept headers are initialized.
        mimetype = "application/json"
        if path is not None and path.endswith("/_bulk"):
            # Server version 9 is picky about this. This should improve compatibility.
            mimetype = "application/x-ndjson"
        for header in ("content-type", "accept"):
            headers.setdefault(header, mimetype)

    if compatibility_mode is not None:
        # It ensures compatibility mode is being applied to mime type.
        for header in ("accept", "content-type"):
            headers[header] = _COMPAT_MIMETYPE_RE.sub(
                "application/vnd.elasticsearch+%s; compatible-with=%s" % (r"\g<1>", compatibility_mode), headers[header]
            )
    return headers


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
