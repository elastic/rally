Running Rally with Docker
=========================

Rally is available as a `Docker image <https://hub.docker.com/r/elastic/rally>`_.

.. _docker_limitations:

Limitations
-----------

The following Rally functionality isn't supported when using the Docker image:

* :ref:`Distributing the load test driver <recipe_distributed_load_driver>` to apply load from multiple machines.
* Using other :doc:`pipelines <pipelines/>` apart from ``benchmark-only``.

Quickstart
----------

You can test the Rally Docker image by first issuing a simple command to list the available tracks::

    $ docker run elastic/rally list tracks

        ____        ____
       / __ \____ _/ / /_  __
      / /_/ / __ `/ / / / / /
     / _, _/ /_/ / / / /_/ /
    /_/ |_|\__,_/_/_/\__, /
                    /____/

    Available tracks:

    Name           Description                                                                                                                                                                        Documents    Compressed Size    Uncompressed Size    Default Challenge        All Challenges
    -------------  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------  -----------  -----------------  -------------------  -----------------------  ---------------------------------------------------------------------------------------------------------------------------
    geopoint       Point coordinates from PlanetOSM                                                                                                                                                   60,844,404   481.9 MB           2.3 GB               append-no-conflicts      append-no-conflicts,append-no-conflicts-index-only,append-fast-with-conflicts
    eventdata      This benchmark indexes HTTP access logs generated based sample logs from the elastic.co website using the generator available in https://github.com/elastic/rally-eventdata-track  20,000,000   755.1 MB           15.3 GB              append-no-conflicts      append-no-conflicts
    nested         StackOverflow Q&A stored as nested docs                                                                                                                                            11,203,029   663.1 MB           3.4 GB               nested-search-challenge  nested-search-challenge,index-only
    so             Indexing benchmark using up to questions and answers from StackOverflow                                                                                                            36,062,278   8.9 GB             33.1 GB              append-no-conflicts      append-no-conflicts
    geoshape       Shapes from PlanetOSM                                                                                                                                                              60,523,283   13.4 GB            45.4 GB              append-no-conflicts      append-no-conflicts
    http_logs      HTTP server log data                                                                                                                                                               247,249,096  1.2 GB             31.1 GB              append-no-conflicts      append-no-conflicts,append-no-conflicts-index-only,append-sorted-no-conflicts,append-index-only-with-ingest-pipeline,update
    geonames       POIs from Geonames                                                                                                                                                                 11,396,505   252.4 MB           3.3 GB               append-no-conflicts      append-no-conflicts,append-no-conflicts-index-only,append-sorted-no-conflicts,append-fast-with-conflicts
    noaa           Global daily weather measurements from NOAA                                                                                                                                        33,659,481   947.3 MB           9.0 GB               append-no-conflicts      append-no-conflicts,append-no-conflicts-index-only
    percolator     Percolator benchmark based on AOL queries                                                                                                                                          2,000,000    102.7 kB           104.9 MB             append-no-conflicts      append-no-conflicts
    nyc_taxis      Taxi rides in New York in 2015                                                                                                                                                     165,346,692  4.5 GB             74.3 GB              append-no-conflicts      append-no-conflicts,append-no-conflicts-index-only,append-sorted-no-conflicts-index-only,update,append-ml
    geopointshape  Point coordinates from PlanetOSM indexed as geoshapes                                                                                                                              60,844,404   470.5 MB           2.6 GB               append-no-conflicts      append-no-conflicts,append-no-conflicts-index-only,append-fast-with-conflicts
    metricbeat     Metricbeat data                                                                                                                                                                    1,079,600    87.6 MB            1.2 GB               append-no-conflicts      append-no-conflicts
    pmc            Full text benchmark with academic papers from PMC                                                                                                                                  574,199      5.5 GB             21.7 GB              append-no-conflicts      append-no-conflicts,append-no-conflicts-index-only,append-sorted-no-conflicts,append-fast-with-conflicts

    -------------------------------
    [INFO] SUCCESS (took 3 seconds)
    -------------------------------


As a next step, we assume that Elasticsearch is running on ``es01:9200`` and is accessible from the host where you are running the Rally Docker image.
Run the ``nyc_taxis`` track in ``test-mode`` using::

    $ docker run elastic/rally race --track=nyc_taxis --test-mode --pipeline=benchmark-only --target-hosts=es01:9200


.. note::
    We didn't need to explicitly specify ``esrally`` as we'd normally do in a normal CLI invocation; the entrypoint in the Docker image does this automatically.

Now you are able to use all regular :doc:`Rally commands <command_line_reference/>`, bearing in mind the aforementioned :ref:`limitations <docker_limitations>`.

Configuration
-------------

The Docker image ships with a default configuration file under ``/rally/.rally/rally.ini``.
To customize Rally you can create your own ``rally.ini`` and bind mount it using::

    docker run -v /home/<myuser>/custom_rally.ini:/rally/.rally/rally.ini elastic/rally ...

Persistence
-----------

It is highly recommended to use a local bind mount (or a `named volume <https://docs.docker.com/storage/>`_) for the directory ``/rally/.rally`` in the container.
This will ensure you have persistence across invocations and any tracks downloaded and extracted can be reused, reducing the startup time.
You need to ensure the UID is ``1000`` (or GID is ``0`` especially in OpenShift) so that Rally can write to the bind-mounted directory.

If your local bind mount doesn't contain a ``rally.ini`` the container will create one for you during the first run.

Example::

    mkdir myrally
    sudo chgrp 0 myrally

    # First run will also generate the rally.ini
    docker run --rm -v $PWD/myrally:/rally/.rally elastic/rally race --track=nyc_taxis --test-mode --pipeline=benchmark-only --target-hosts=es01:9200

    ...

    # inspect results
    $ tree myrally/benchmarks/races/
    myrally/benchmarks/races/
    └── 1d81930a-4ebe-4640-a09b-3055174bce43
        └── race.json

    1 directory, 1 file


In case you forgot to bind mount a directory, the Rally Docker image will create an `anonymous volume <https://docs.docker.com/storage/>`_ for ``/rally/.rally`` to ensure logs and results get persisted even after the container has terminated.

For example, after executing our earlier quickstart example ``docker run elastic/rally race --track=nyc_taxis --test-mode --pipeline=benchmark-only --target-hosts=es01:9200``, ``docker volume ls`` shows a volume::

    $ docker volume ls
    DRIVER              VOLUME NAME
    local               96256462c3a1f61120443e6d69d9cb0091b28a02234318bdabc52b6801972199


To further examine the contents we can bind mount it from another image e.g.::

    $ docker run --rm -i -v=96256462c3a1f61120443e6d69d9cb0091b28a02234318bdabc52b6801972199:/rallyvolume -ti python:3.8.2-slim /bin/bash
    root@9a7dd7b3d8df:/# cd /rallyvolume/
    root@9a7dd7b3d8df:/rallyvolume# ls
    root@9a7dd7b3d8df:/rallyvolume/.rally# ls
    benchmarks  logging.json  logs	rally.ini
    # head -4 benchmarks/races/1d81930a-4ebe-4640-a09b-3055174bce43/race.json
    {
     "rally-version": "1.2.1.dev0",
     "environment": "local",
     "race-id": "1d81930a-4ebe-4640-a09b-3055174bce43",

Specifics about the image
-------------------------

Rally runs as user ``1000`` and its files are installed with uid:gid ``1000:0`` (to support `Arbitrary User IDs <https://docs.openshift.com/container-platform/3.11/creating_images/guidelines.html>`_).

Extending the Docker image
--------------------------

You can also create your own customized Docker image on top of the existing one.
The example below shows how to get started::

    FROM elastic/rally:1.2.1
    COPY --chown=1000:0 rally.ini /rally/.rally/

You can then build and test the image with::

    docker build --tag=custom-rally .
    docker run -ti custom-rally list tracks
