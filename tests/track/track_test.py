from unittest import TestCase

from esrally.track import track


class TrackTests(TestCase):
    def test_finds_default_challenge(self):
        default_challenge = track.Challenge("default", description="default challenge", default=True)
        another_challenge = track.Challenge("default", description="default challenge", default=False)

        self.assertEqual(default_challenge,
                         track.Track(name="unittest",
                                     short_description="unittest track",
                                     description="unittest track",
                                     challenges=[another_challenge, default_challenge])
                         .default_challenge)

    def test_default_challenge_none_if_no_challenges(self):
        self.assertIsNone(track.Track(name="unittest",
                                      short_description="unittest track",
                                      description="unittest track",
                                      challenges=[])
                          .default_challenge)

    def test_default_challenge_none_if_no_challenges(self):
        self.assertIsNone(track.Track(name="unittest",
                                      short_description="unittest track",
                                      description="unittest track",
                                      challenges=[])
                          .default_challenge)
