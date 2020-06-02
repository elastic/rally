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

.. note::

   Rally Daemon will listen on port 1900 and the actor system that Rally uses internally require access to arbitrary (unprivileged) ports. Be sure to open up these ports between the Rally nodes.

Starting
--------

For all this to work, Rally needs to form a cluster. This is achieved with the binary ``esrallyd`` (note the "d" - for daemon - at the end). You need to start the Rally daemon on all nodes: First on the coordinator node, then on all others. The order does matter, because nodes attempt to connect to the coordinator on startup.

On the benchmark coordinator, issue::

    esrallyd start --node-ip=IP_OF_COORDINATOR_NODE --coordinator-ip=IP_OF_COORDINATOR_NODE


On all other nodes, issue::

    esrallyd start --node-ip=IP_OF_THIS_NODE --coordinator-ip=IP_OF_COORDINATOR_NODE

After that, all Rally nodes, know about each other and you can use Rally as usual. See the :doc:`tips and tricks </recipes>` for more examples.

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

Rally uses the actor system `Thespian <https://github.com/kquick/Thespian>`_ under the hood.

At startup, `Thespian attempts to detect an appropriate IP address <https://thespianpy.com/doc/using#hH-9d33a877-b4f0-4012-9510-442d81b0837c>`_. If Rally fails to startup the actor system indicated by the following message::

    thespian.actors.InvalidActorAddress: ActorAddr-(T|:1900) is not a valid ActorSystem admin

then set a routable IP address yourself by setting the environment variable ``THESPIAN_BASE_IPADDR`` before starting Rally.

.. note::

   This issue often occurs when Rally is started on a machine that is connected via a VPN to the Internet. We advise against such a setup for benchmarking and suggest to setup the load generator and the target machines close to each other, ideally in the same subnet.


To inspect Thespian's status in more detail you can use the `Thespian shell <https://thespianpy.com/doc/in_depth.html#hH-058d8939-b973-4270-975b-3afd9c607176>`_. Below is an example invocation that demonstrates how to retrieve the actor system status::

    python3 -m thespian.shell
    Thespian Actor shell.  Type help or '?' to list commands.'

    thespian> start multiprocTCPBase
    Starting multiprocTCPBase ActorSystem
    Capabilities: {}
    Started multiprocTCPBase ActorSystem
    thespian> address localhost 1900
    Args is: {'port': '1900', 'ipaddr': 'localhost'}
    Actor Address 0:  ActorAddr-(T|:1900)
    thespian> status
    Requesting status from Actor (or Admin) @ ActorAddr-(T|:1900) (#0)
    Status of ActorSystem @ ActorAddr-(T|192.168.14.2:1900) [#1]:
      |Capabilities[9]:
                                   ip: 192.168.14.2
              Convention Address.IPv4: 192.168.14.2:1900
                  Thespian Generation: (3, 9)
             Thespian Watch Supported: True
                       Python Version: (3, 5, 2, 'final', 0)
            Thespian ActorSystem Name: multiprocTCPBase
         Thespian ActorSystem Version: 2
                     Thespian Version: 1581669778176
                          coordinator: True
      |Convention Leader: ActorAddr-(T|192.168.14.2:1900) [#1]
      |Convention Attendees [3]:
        @ ActorAddr-(T|192.168.14.4:1900) [#2]: Expires_in_0:21:41.056599
        @ ActorAddr-(T|192.168.14.3:1900) [#3]: Expires_in_0:21:41.030934
        @ ActorAddr-(T|192.168.14.5:1900) [#4]: Expires_in_0:21:41.391251
      |Primary Actors [0]:
      |Rate Governer: Rate limit: 4480 messages/sec (currently low with 1077 ticks)
      |Pending Messages [0]:
      |Received Messages [0]:
      |Pending Wakeups [0]:
      |Pending Address Resolution [0]:
      |>        1077 - Actor.Message Send.Transmit Started
      |>          84 - Admin Handle Convention Registration
      |>        1079 - Admin Message Received.Total
      |>           6 - Admin Message Received.Type.QueryExists
      |>         988 - Admin Message Received.Type.StatusReq
      |> sock#0-fd10 - Idle-socket <socket.socket fd=10, family=AddressFamily.AF_INET, type=2049, proto=6, laddr=('192.168.14.2', 1900), raddr=('192.168.14.4', 44024)>->ActorAddr-(T|192.168.14.4:1900) (Expires_in_0:19:35.060480)
      |> sock#2-fd11 - Idle-socket <socket.socket fd=11, family=AddressFamily.AF_INET, type=2049, proto=6, laddr=('192.168.14.2', 1900), raddr=('192.168.14.3', 40244)>->ActorAddr-(T|192.168.14.3:1900) (Expires_in_0:19:35.034779)
      |> sock#3-fd12 - Idle-socket <socket.socket fd=12, family=AddressFamily.AF_INET, type=2049, proto=6, laddr=('192.168.14.2', 1900), raddr=('192.168.14.5', 58358)>->ActorAddr-(T|192.168.14.5:1900) (Expires_in_0:19:35.394918)
      |> sock#1-fd13 - Idle-socket <socket.socket fd=13, family=AddressFamily.AF_INET, type=2049, proto=6, laddr=('127.0.0.1', 1900), raddr=('127.0.0.1', 34320)>->ActorAddr-(T|:46419) (Expires_in_0:19:59.999337)
      |DeadLetter Addresses [0]:
      |Source Authority: None
      |Loaded Sources [0]:
      |Global Actors [0]:


