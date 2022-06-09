from unittest.mock import MagicMock, patch

import pytest

from esrally import rally


@pytest.mark.parametrize(
    "offline,has_conn,subcommand,expected",
    [
        # command that needs internet
        (True, True, "info", True),
        (True, False, "info", True),
        (False, True, "info", True),
        (False, False, "info", False),
        # command that doesn't need internet
        (True, True, "list", True),
        (True, False, "list", True),
        (False, True, "list", True),
        (False, False, "list", True),
    ],
)
def test_ensure_internet_connection(offline, has_conn, subcommand, expected):
    args = MagicMock(offline=offline, subcommand=subcommand)
    logger = MagicMock()
    cfg = MagicMock()

    with patch("esrally.rally.net.has_internet_connection", MagicMock(return_value=has_conn)):
        assert rally.ensure_internet_connection(args, cfg, logger) == expected
