### Bug Fixes

* Test mode (`--test-mode`) now runs iteration-based tasks with no warmup iterations and a single measurement iteration per client. Previously, each client ran as many warmup and measurement iterations as the task had clients.
