Rally Daemon
============

At its heart, Rally is a distributed system, just like Elasticsearch. However, in its simplest form you will not notice, because all components of Rally can run on a single node too. If you want Rally to :ref:`configure and start Elasticsearch nodes remotely <recipe_benchmark_remote_cluster>` or :ref:`distribute the load test driver <recipe_distributed_load_driver>` to apply load from multiple machines, you need to use Rally daemon.

Rally daemon needs to run on every machine that should be under Rally's control. We can consider three different roles:

* Benchmark coordinator: This is the machine where you invoke ``esrally``. It is responsible for user interaction, coordinates the whole benchmark and shows the results. Only one node can be the benchmark coordinator.
* Load driver: Nodes of this type will interpret and run :doc:`tracks </track>`.
* Provisioner: Nodes of this type will configure an Elasticsearch cluster according to the provided :doc:`car </car>` and :doc:`Elasticsearch plugin </elasticsearch_plugins>` configurations.

The two latter roles are not statically preassigned but rather determined by Rally based on the command line parameter ``--load-driver-hosts`` (for the load driver) and ``--target-hosts`` (for the provisioner).

Rally runs its components as actors on `Ray <https://docs.ray.io/en/latest/ray-core/walkthrough.html>`_. The Rally daemon is a Ray node: the daemon on the benchmark coordinator is the head of a Ray cluster, and the daemons on all other machines join that cluster.

Preparation
-----------

First, :doc:`install </install>` and :doc:`configure </configuration>` Rally on all machines that are involved in the benchmark. Use the same version of Rally *and of Python* on all machines: Ray refuses to form a cluster of nodes with different versions. If you want to automate this, there is no need to use the interactive configuration routine of Rally. You can copy `~/.rally/rally.ini` to the target machines adapting the paths in the file as necessary. We also recommend that you copy ``~/.rally/benchmarks/data`` to all load driver machines before-hand. Otherwise, each load driver machine will need to download a complete copy of the benchmark data.

The Rally daemon must be started from the same Python environment (e.g. virtualenv) as ``esrally`` because it starts Rally's actors on its machine.

Network
~~~~~~~

The Rally daemon on the benchmark coordinator listens on port 1900. All Rally nodes also need to reach each other on further ports: Ray's node manager, object manager and agents listen on random ports by default, and Ray's worker processes use ports 10002 to 19999. See `Ray's port configuration <https://docs.ray.io/en/latest/ray-core/configure.html#ports-configurations>`_ for details. Be sure to open up these ports between the Rally nodes, ideally by allowing all TCP traffic between them.

Authentication
~~~~~~~~~~~~~~

All Rally nodes and ``esrally`` authenticate with each other using a shared token. When you start the Rally daemon on the benchmark coordinator, it creates the token in ``~/.ray/auth_token`` unless the file exists already. Copy this file to the same location on all other machines before you start the Rally daemon on them. You can also provide the token with the environment variable ``RAY_AUTH_TOKEN`` or the path to it with ``RAY_AUTH_TOKEN_PATH``. See `Ray's token authentication <https://docs.ray.io/en/latest/ray-security/token-auth.html>`_ for details.

.. warning::

   The token is transmitted unencrypted. Only run Rally on networks that you trust.

To disable authentication, set the environment variable ``RAY_AUTH_MODE=disabled`` on all machines, for both ``esrallyd`` and ``esrally``. The setting must be the same everywhere, otherwise nodes cannot connect to each other. Without authentication, anybody who can connect to the Rally nodes can run code on them, as was the case before Rally used Ray.

Starting
--------

For all this to work, Rally needs to form a cluster. This is achieved with the binary ``esrallyd`` (note the "d" - for daemon - at the end). You need to start the Rally daemon on all nodes: First on the coordinator node, then on all others. The order does matter, because nodes connect to the coordinator on startup.

On the benchmark coordinator, issue::

    esrallyd start --node-ip=IP_OF_COORDINATOR_NODE --coordinator-ip=IP_OF_COORDINATOR_NODE

Then copy ``~/.ray/auth_token`` from the benchmark coordinator to all other nodes (see above) and issue on all other nodes::

    esrallyd start --node-ip=IP_OF_THIS_NODE --coordinator-ip=IP_OF_COORDINATOR_NODE

After that, all Rally nodes know about each other and you can use Rally as usual. Rally places its actors on nodes by IP address, so use the IP addresses given as ``--node-ip`` in ``--load-driver-hosts`` and ``--target-hosts``. See the :doc:`tips and tricks </recipes>` for more examples.

Stopping
--------

You can leave the Rally daemon processes running in case you want to run multiple benchmarks. When you are done, you can stop the Rally daemon on each node with::

    esrallyd stop

Contrary to startup, order does not matter here.

Status
------

You can query the status of the local Rally daemon with::

    esrallyd status

Troubleshooting
---------------

Rally uses `Ray <https://docs.ray.io/>`__ under the hood. ``esrallyd start`` and ``esrallyd stop`` run ``ray start`` and ``ray stop``; their output is written to Rally's log file.

To inspect the cluster that the Rally daemons have formed, run the following command on any node of the cluster (in the same Python environment as Rally)::

    RAY_AUTH_MODE=token ray status --address=IP_OF_COORDINATOR_NODE:1900

It shows all nodes that have joined the cluster. If a node is missing, check that the Rally daemon runs on it, that it uses the same token as the benchmark coordinator and that the network allows traffic between the nodes.

``RAY_AUTH_MODE=token`` makes ``ray`` authenticate with the cluster's token, which Rally does by default. It may take a few seconds after starting the daemon until ``ray status`` reports the cluster.

Ray writes its own log files to ``/tmp/ray/session_latest/logs`` on each machine (set the environment variable ``RAY_TMPDIR`` to use another directory than ``/tmp``). Check them when Rally's actors fail to start or to communicate. Rally's log files contain the name of the actor that wrote each log line (e.g. ``worker-3``).

If ``esrallyd stop`` fails to stop the daemon, you can stop all Ray processes on a machine with::

    ray stop --force
