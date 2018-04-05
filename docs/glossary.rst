Glossary
========

.. glossary::

    track
        A :doc:`track </track>` is the description of one or more benchmarking scenarios with a specific document corpus. It defines for example the involved indices, data files and which operations are invoked. List the available tracks with ``esrally list tracks``. Although Rally ships with some tracks out of the box, you should usually :doc:`create your own track</adding_tracks>` based on your own data.

    challenge
        A challenge describes one benchmarking scenario, for example indexing documents at maximum throughput with 4 clients while issuing term and phrase queries from another two clients rate-limited at 10 queries per second each. It is always specified in the context of a track. See the available challenges by listing the corresponding tracks with ``esrally list tracks``.

    car
        A :doc:`car </car>` is a specific configuration of an Elasticsearch cluster that is benchmarked, for example the out-of-the-box configuration, a configuration with a specific heap size or a custom logging configuration. List the available cars with ``esrally list cars``.

    telemetry
        :doc:`Telemetry </telemetry>` is used in Rally to gather metrics about the car, for example CPU usage or index size.

    race
        A :doc:`race </race>` is one invocation of the Rally binary. Another name for that is one "benchmarking trial". During a race, Rally runs one challenge on a track with the given car.

    tournament
        A tournament is a comparison of two races. You can use Rally's :doc:`tournament mode </tournament>` for that.
