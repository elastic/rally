### Breaking Changes

* Rally now runs the Elasticsearch it provisions with the bundled JDK by default and ignores `JAVA_HOME`. Pass `--runtime-jdk=<major>` to use a JDK from `JAVAx_HOME` or `JAVA_HOME` instead.
