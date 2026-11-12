### Enhancements

* Add `--telemetry-client-options` to `race` to collect telemetry with different credentials than the benchmark. On Elasticsearch Serverless, telemetry devices that require operator privileges now follow the telemetry user's operator status, while challenge tasks keep using the restricted `--client-options` credentials.

### Bug Fixes

* Rally no longer writes `--client-options` credentials to its debug log. The command line and parsed arguments that Rally logs at startup were previously unredacted, and the redaction used when logging other Rally processes missed `--client-options <value>`.
