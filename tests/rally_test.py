from unittest import mock

import pytest

from esrally import exceptions, rally


class TestConfigureTrackParams:
    def test_valid_date_range(self):
        cfg = mock.Mock()
        args = mock.Mock()
        args.track_params = "start_date:2023-01-01,end_date:2023-01-02"
        args.track_path = None
        args.track_revision = None
        args.track = "test-track"
        args.track_repository = "default"
        args.challenge = "test-challenge"
        args.include_tasks = None
        args.exclude_tasks = None

        rally.configure_track_params(mock.Mock(), args, cfg)

        # Check if params were added to config
        assert cfg.add.call_count > 0

    def test_invalid_date_range_start_equals_end(self):
        cfg = mock.Mock()
        args = mock.Mock()
        args.track_params = "start_date:2023-01-01,end_date:2023-01-01"
        args.track_path = None
        args.track_revision = None
        args.track = "test-track"
        args.track_repository = "default"
        args.challenge = "test-challenge"
        args.include_tasks = None
        args.exclude_tasks = None

        with pytest.raises(exceptions.SystemSetupError, match="track-param 'start_date' .* must be earlier than 'end_date'"):
            rally.configure_track_params(mock.Mock(), args, cfg)

    def test_invalid_date_range_start_after_end(self):
        cfg = mock.Mock()
        args = mock.Mock()
        args.track_params = "start_date:2023-01-02,end_date:2023-01-01"
        args.track_path = None
        args.track_revision = None
        args.track = "test-track"
        args.track_repository = "default"
        args.challenge = "test-challenge"
        args.include_tasks = None
        args.exclude_tasks = None

        with pytest.raises(exceptions.SystemSetupError, match="track-param 'start_date' .* must be earlier than 'end_date'"):
            rally.configure_track_params(mock.Mock(), args, cfg)

    def test_valid_date_range_iso_format(self):
        cfg = mock.Mock()
        args = mock.Mock()
        args.track_params = '{"start_date":"2023-01-01T10:00:00","end_date":"2023-01-01T11:00:00"}'
        args.track_path = None
        args.track_revision = None
        args.track = "test-track"
        args.track_repository = "default"
        args.challenge = "test-challenge"
        args.include_tasks = None
        args.exclude_tasks = None

        rally.configure_track_params(mock.Mock(), args, cfg)
        assert cfg.add.call_count > 0

    def test_invalid_date_range_iso_format(self):
        cfg = mock.Mock()
        args = mock.Mock()
        args.track_params = '{"start_date":"2023-01-01T12:00:00","end_date":"2023-01-01T11:00:00"}'
        args.track_path = None
        args.track_revision = None
        args.track = "test-track"
        args.track_repository = "default"
        args.challenge = "test-challenge"
        args.include_tasks = None
        args.exclude_tasks = None

        with pytest.raises(exceptions.SystemSetupError, match="track-param 'start_date' .* must be earlier than 'end_date'"):
            rally.configure_track_params(mock.Mock(), args, cfg)
