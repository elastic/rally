Recipes
=======

Benchmarking an existing cluster
--------------------------------

.. warning::

    If you are just getting started with Rally and don't understand how it works, please do NOT run it against any production or production-like cluster. Besides, benchmarks should be executed in a dedicated environment anyway where no additional traffic skews results.

.. note::

    We assume in this recipe, that Rally is already properly :doc:`configured </configuration>`.

Consider the following configuration: You have an existing benchmarking cluster, that consists of three Elasticsearch nodes running on ``10.5.5.10``, ``10.5.5.11``, ``10.5.5.12``. You've setup the cluster yourself and want to benchmark it with Rally. Rally is installed on ``10.5.5.5``.

.. image:: benchmark_existing.png
:alt: Sample Benchmarking Scenario

First of all, we need to decide on a track. So, we run ``esrally list tracks``::

    Name        Description                                                           Challenges
    ----------  --------------------------------------------------------------------  -----------------------------------------------------------------------------------------------------------------------------------------------
    geonames    Standard benchmark in Rally (8.6M POIs from Geonames)                 append-no-conflicts,append-no-conflicts-index-only,append-no-conflicts-index-only-1-replica,append-fast-no-conflicts,append-fast-with-conflicts
    geopoint    60.8M POIs from PlanetOSM                                             append-no-conflicts,append-no-conflicts-index-only,append-no-conflicts-index-only-1-replica,append-fast-no-conflicts,append-fast-with-conflicts
    logging     Logging benchmark                                                     append-no-conflicts,append-no-conflicts-index-only,append-no-conflicts-index-only-1-replica,append-fast-no-conflicts,append-fast-with-conflicts
    nyc_taxis   Trip records completed in yellow and green taxis in New York in 2015  append-no-conflicts,append-no-conflicts-index-only,append-no-conflicts-index-only-1-replica
    percolator  Percolator benchmark based on 2M AOL queries                          append-no-conflicts
    pmc         Full text benchmark containing 574.199 papers from PMC                append-no-conflicts,append-no-conflicts-index-only,append-no-conflicts-index-only-1-replica,append-fast-no-conflicts,append-fast-with-conflicts

We're interested in a full text benchmark, so we'll choose to run ``pmc``. If you have your own data that you want to use for benchmarks, then please :doc:`create your own track</adding_tracks>` instead; the metrics you'll gather which be representative and much more useful than some default track.

Next, we need to know which machines to target which is easy as we can see that from the diagram above.

Finally we need to check which :doc:`pipeline </pipelines>` to use. For this case, the ``benchmark-only`` pipeline is suitable as we don't want Rally to provision the cluster for us.

Now we can invoke Rally::

    esrally --track=pmc --target-hosts=10.5.5.10:9200,10.5.5.11:9200,10.5.5.12:9200 --pipeline=benchmark-only

If you have `X-Pack Security <https://www.elastic.co/products/x-pack/security>`_  enabled, then you'll also need to specify another parameter to use https and to pass credentials::

    esrally --track=pmc --target-hosts=10.5.5.10:9243,10.5.5.11:9243,10.5.5.12:9243 --pipeline=benchmark-only --client-options="basic_auth_user:'elastic',basic_auth_password:'changeme'"

Changing the default track repository
-------------------------------------

Rally supports multiple track repositories. This allows you for example to have a separate company-internal repository for your own tracks that is separate from `Rally's default track repository <https://github.com/elastic/rally-tracks>`_. However, you always need to define ``--track-repository=my-custom-repository`` which can be cumbersome. If you want to avoid that and want Rally to use your own track repository by default you can just replace the default track repository definition in ``~./rally/rally.ini``. Consider this example::

    ...
    [tracks]
    default.url = git@github.com:elastic/rally-tracks.git
    teamtrackrepo.url = git@example.org/myteam/my-tracks.git

If ``teamtrackrepo`` should be the default track repository, just define it as ``default.url``. E.g.::

    ...
    [tracks]
    default.url = git@example.org/myteam/my-tracks.git
    old-rally-default.url=git@github.com:elastic/rally-tracks.git

Also don't forget to rename the folder of your local working copy as Rally will search for a track repository with the name ``default``::

    cd ~/.rally/benchmarks/tracks/
    mv default old-rally-default
    mv teamtrackrepo default

From now on, Rally will treat your repository as default and you need to run Rally with ``--track-repository=old-rally-default`` if you want to use the out-of-the-box Rally tracks.
