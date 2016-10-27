Cars
====

A rally "car" is a specific configuration of Elasticsearch. At the moment, Rally ships with a fixed set of cars that you cannot change. You can list them with ``esrally list cars``::

        ____        ____
       / __ \____ _/ / /_  __
      / /_/ / __ `/ / / / / /
     / _, _/ /_/ / / / /_/ /
    /_/ |_|\__,_/_/_/\__, /
                    /____/

    Available cars:

    Name
    ----------
    defaults
    4gheap
    two_nodes
    verbose_iw

You can specify the car that Rally should use with e.g. ``--car=4gheap``.