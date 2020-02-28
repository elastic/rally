Tracker Reference
^^^^^^^^^^^^^^^^^

Intro
-----
Tracker is a Rally utility for generating a Rally track template + corpus from one or more existing Elasticsearch
indices.

Usage
-----
To generate a new track, invoke Tracker as follows (replacing the example parameters below)::

    estracker --target-hosts=host:9200,host2:9200 --client-options="use_ssl:true,verify_certs:false,basic_auth_user:'rally',basic_auth_password:'rally-password'" --indices index_1,index_2 --track-name=xtrack --outdir=mytracks

Note above that target-hosts and client-options parameters are the same as used in Rally.

For each index on the specified Elasticsearch cluster, Tracker will retrieve the mapping and then proceed to fetch all
documents in the index, saving the attached `_source` field for each document.

When Tracker has finished, a folder with the name of your track will be created in the specified output directory::

    > find tracks/xtrack
    tracks/xtrack
    tracks/xtrack/challenges
    tracks/xtrack/challenges/default.json
    tracks/xtrack/index_1-documents.json
    tracks/xtrack/index_1-documents.json.bz2
    tracks/xtrack/index_1.json
    tracks/xtrack/track.json

Here's a rundown of what Tracker produces:

 - track.json as described in the :doc:`track reference<track>`, the manifest for the new track
 - index_1.json - mapping and settings for the extracted index
 - index_1-documents.json(.bz2) - the sources of all the documents from the extracted index, also compressed
   for storage efficiency
 - challenges/default.json - a default "ingest" challenge to get you started by re-creating the extracted index

