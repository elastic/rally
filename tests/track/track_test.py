from unittest import TestCase

from esrally.track import track


class TrackTests(TestCase):
    def test_finds_default_challenge(self):
        default_challenge = track.Challenge("default", description="default challenge", default=True)
        another_challenge = track.Challenge("other", description="non-default challenge", default=False)

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

    def test_finds_challenge_by_name(self):
        default_challenge = track.Challenge("default", description="default challenge", default=True)
        another_challenge = track.Challenge("other", description="non-default challenge", default=False)

        self.assertEqual(another_challenge,
                         track.Track(name="unittest",
                                     short_description="unittest track",
                                     description="unittest track",
                                     challenges=[another_challenge, default_challenge])
                         .find_challenge_or_default("other"))

    def test_uses_default_challenge_if_no_name_given(self):
        default_challenge = track.Challenge("default", description="default challenge", default=True)
        another_challenge = track.Challenge("other", description="non-default challenge", default=False)

        self.assertEqual(default_challenge,
                         track.Track(name="unittest",
                                     short_description="unittest track",
                                     description="unittest track",
                                     challenges=[another_challenge, default_challenge])
                         .find_challenge_or_default(""))

    def test_does_not_find_unknown_challenge(self):
        default_challenge = track.Challenge("default", description="default challenge", default=True)
        another_challenge = track.Challenge("other", description="non-default challenge", default=False)

        self.assertIsNone(track.Track(name="unittest",
                                      short_description="unittest track",
                                      description="unittest track",
                                      challenges=[another_challenge, default_challenge])
                          .find_challenge_or_default("unknown-name"))
