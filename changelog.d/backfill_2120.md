### Breaking Changes

* [#2120](https://github.com/elastic/rally/pull/2120): Tracks that specify `types`, `target-type` or a search `type` now fail to load. Remove these fields, since Elasticsearch 8.0+ rejects mapping types anyway.
