Rally Daemon
============

At its heart, Rally is a distributed system, just like Elasticsearch. However, in its simplest form you will not notice, because all components of Rally can run on a single node too. If you want Rally to :ref:`configure and start Elasticsearch nodes remotely <recipe_benchmark_remote_cluster>` or :ref:`distribute the load test driver <recipe_distributed_load_driver>` to apply load from multiple machines, you need to use Rally daemon.

Rally daemon needs to run on every machine that should be under Rally's control. We can consider three different roles:

* Benchmark coordinator: This is the machine where you invoke ``esrally``. It is responsible for user interaction, coordinates the whole benchmark and shows the results. Only one node can be the benchmark coordinator.
* Load driver: Nodes of this type will interpret and run :doc:`tracks </track>`.
* Provisioner: Nodes of this type will configure an Elasticsearch cluster according to the provided :doc:`car </car>` and :doc:`Elasticsearch plugin </elasticsearch_plugins>` configurations.

The two latter roles are not statically preassigned but rather determined by Rally based on the command line parameter ``--load-driver-hosts`` (for the load driver) and ``--target-hosts`` (for the provisioner).

Preparation
-----------

First, :doc:`install </install>` and :doc:`configure </configuration>` Rally on all machines that are involved in the benchmark. If you want to automate this, there is no need to use the interactive configuration routine of Rally. You can copy `~/.rally/rally.ini` to the target machines adapting the paths in the file as necessary. We also recommend that you copy ``~/.rally/benchmarks/data`` to all load driver machines before-hand. Otherwise, each load driver machine will need to download a complete copy of the benchmark data.

Starting
--------

For all this to work, Rally needs to form a cluster. This is achieved with the binary ``esrallyd`` (note the "d" - for daemon - at the end). You need to start the Rally daemon on all nodes: First on the coordinator node, then on all others. The order does matter, because nodes attempt to connect to the coordinator on startup.

On the benchmark coordinator, issue::

    esrallyd start --node-ip=IP_OF_COORDINATOR_NODE --coordinator-ip=IP_OF_COORDINATOR_NODE


On all other nodes, issue::

    esrallyd start --node-ip=IP_OF_THIS_NODE --coordinator-ip=IP_OF_COORDINATOR_NODE

After that, all Rally nodes, know about each other and you can use Rally as usual. Please see the :doc:`recipes </recipes>` for more examples.

Stopping
--------

You can leave the Rally daemon processes running in case you want to run multiple benchmarks. When you are done, you can stop the Rally daemon on each node with::

    esrallyd stop

Contrary to startup, order does not matter here.
