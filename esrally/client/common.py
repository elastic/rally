import re
from collections.abc import Mapping, MutableMapping
from datetime import date, datetime
from typing import Any

from elastic_transport.client_utils import percent_encode
from elasticsearch import VERSION as ES_VERSION

from esrally.utils import versions

_MAJOR_SERVER_VERSION = str(ES_VERSION[0])
_WARNING_RE = re.compile(r"\"([^\"]*)\"")
_COMPAT_MIMETYPE_RE = re.compile(r"application/(json|x-ndjson|vnd\.mapbox-vector-tile)")


def mimetype_headers_to_compat(headers: MutableMapping[str, str], distribution_version: str | None) -> None:
    if not headers:
        return
    if not versions.is_version_identifier(distribution_version):
        return
    major_version = versions.Version.from_string(distribution_version).major
    if major_version < 8:
        return

    for header in ("accept", "content-type"):
        mimetype = headers.get(header)
        if not mimetype:
            continue
        headers[header] = _COMPAT_MIMETYPE_RE.sub(
            "application/vnd.elasticsearch+%s; compatible-with=%s" % (r"\g<1>", major_version), mimetype
        )


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
