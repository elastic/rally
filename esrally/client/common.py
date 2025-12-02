import re
from collections.abc import Mapping
from datetime import date, datetime
from typing import Any

from elastic_transport.client_utils import percent_encode
from elasticsearch import VERSION


def _client_major_version_to_str(version: tuple) -> str:
    return str(version[0])


_WARNING_RE = re.compile(r"\"([^\"]*)\"")
_COMPAT_MIMETYPE_TEMPLATE = "application/vnd.elasticsearch+%s; compatible-with=" + _client_major_version_to_str(VERSION)
_COMPAT_MIMETYPE_RE = re.compile(r"application/(json|x-ndjson|vnd\.mapbox-vector-tile)")
_COMPAT_MIMETYPE_SUB = _COMPAT_MIMETYPE_TEMPLATE % (r"\g<1>",)


def _mimetype_header_to_compat(header, request_headers):
    # Converts all parts of a Accept/Content-Type headers
    # from application/X -> application/vnd.elasticsearch+X
    mimetype = request_headers.get(header, None) if request_headers else None
    if mimetype:
        request_headers[header] = _COMPAT_MIMETYPE_RE.sub(_COMPAT_MIMETYPE_SUB, mimetype)


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
