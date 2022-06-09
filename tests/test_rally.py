from unittest.mock import MagicMock

import pytest

from esrally import rally


@pytest.mark.parametrize(
    "offline,probing_url,subcommand,expected",
    [
        (True, "https://github.com", "info", True),
        (True, "http://invalid", "info", True),
        (False, "https://github.com", "info", True),
        (False, "http://invalid", "info", False),
        (False, "http://github.com", "list", True),
        (False, "http://invalid", "list", True),
    ],
)
def test_ensure_internet_connection(offline, probing_url, subcommand, expected):
    args = MagicMock(offline=offline, subcommand=subcommand)
    logger = MagicMock()
    cfg = MagicMock()
    cfg.opts = MagicMock(return_value=probing_url)

    assert rally.ensure_internet_connection(args, cfg, logger) == expected
