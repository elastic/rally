### Bug Fixes

* `esrally list tracks --format=json` prints JSON again. The option was read from a key only the race listing wrote, so it was ignored for tracks, and an unknown format is now rejected instead of silently falling back to text.
