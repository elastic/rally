### 0.0.3

Major changes:

* Migrated the metrics data store from file-based to a dedicated Elasticsearch instance. Graphical reports can be created with 
  Kibana (optional but recommended). It is necessary to setup an Elasticsearch cluster to store metrics data (a single node 
  is sufficient). The cluster will be configured automatically by Rally. For details please see the [README](README.md).
  
  Related issues: #8, #21, #46, 
  
[All changes](https://github.com/elastic/rally/milestones/0.0.3)