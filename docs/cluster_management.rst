Setting up a Cluster
====================

In this section we cover how to setup an Elasticsearch cluster with Rally. It is by no means required to use Rally for this and you can also use existing tooling like Ansible to achieve the same goal. The main difference between standard tools and Rally is that Rally is capable of setting up a :doc:`wide range of Elasticsearch versions </versions>`.

.. warning::

    The following functionality is experimental. Expect the functionality and the command line interface to change significantly even in patch releases.

Overview
--------

You can use the following subcommands in Rally to manage Elasticsearch nodes:

* ``install`` to install a single Elasticsearch node
* ``start`` to start a previously installed Elasticsearch node
* ``stop`` to stop a running Elasticsearch node and remove the installation

Each command needs to be executed locally on the machine where the Elasticsearch node should run. To setup more complex clusters remotely, we recommend using a tool like Ansible to connect to remote machines and issue these commands via Rally.

Getting Started: Benchmarking a Single Node
-------------------------------------------

In this section we will setup a single Elasticsearch node locally, run a benchmark and then cleanup.

First we need to install Elasticearch::

    esrally install --quiet --distribution-version=7.4.2 --node-name="rally-node-0" --network-host="127.0.0.1" --http-port=39200 --master-nodes="rally-node-0" --seed-hosts="127.0.0.1:39300"

The parameter ``--network-host`` defines the network interface this node will bind to and ``--http-port`` defines which port will be exposed for HTTP traffic. Rally will automatically choose the transport port range as 100 above (39300). The parameters ``--master-nodes`` and ``--seed-hosts`` are necessary for the discovery process. Please see the respective Elasticsearch documentation on `discovery <https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-discovery.html>`_ for more details.

This produces the following output (the value will vary for each invocation)::

    {
      "installation-id": "69ffcfee-6378-4090-9e93-87c9f8ee59a7"
    }


We will need the installation id in the next steps to refer to our current installation.

.. note::

   You can extract the installation id value with `jq <https://stedolan.github.io/jq/>`_ with the following command: ``jq --raw-output '.["installation-id"]'``.

After installation, we can start the node. To tie all metrics of a benchmark together, Rally needs a consistent race id across all invocations. The format of the race id does not matter but we suggest using UUIDs. You can generate a UUID on the command line with ``uuidgen``. Issue the following command to start the node::

    # generate a unique race id (use the same id from now on)
    export RACE_ID=$(uuidgen)
    esrally start --installation-id="69ffcfee-6378-4090-9e93-87c9f8ee59a7" --race-id="${RACE_ID}"

After the Elasticsearch node has started, we can run a benchmark. Be sure to pass the same race id so you can match results later in your metrics store::

    esrally race --pipeline=benchmark-only --target-host=127.0.0.1:39200 --track=geonames --challenge=append-no-conflicts-index-only --on-error=abort --race-id=${RACE_ID}

When the benchmark has finished, we can stop the node again::

    esrally stop --installation-id="69ffcfee-6378-4090-9e93-87c9f8ee59a7"

If you only want to shutdown the node but don't want to delete the node and the data, pass ``--preserve-install`` additionally.


Levelling Up: Benchmarking a Cluster
------------------------------------

This approach of being able to manage individual cluster nodes shows its power when we want to setup a cluster consisting of multiple nodes. At the moment Rally only supports a uniform cluster architecture but with this approach we can also setup arbitrarily complex clusters. The following examples shows how to setup a uniform three node cluster on three machines with the IPs ``192.168.14.77``, ``192.168.14.78`` and ``192.168.14.79``. On each machine we will issue the following command (pick the right one per machine)::

    # on 192.168.14.77
    export INSTALLATION_ID=$(esrally install --quiet --distribution-version=7.4.2 --node-name="rally-node-0" --network-host="192.168.14.77" --http-port=39200 --master-nodes="rally-node-0,rally-node-1,rally-node-2" --seed-hosts="192.168.14.77:39300,192.168.14.78:39300,192.168.14.79:39300" | jq --raw-output '.["installation-id"]')
    # on 192.168.14.78
    export INSTALLATION_ID=$(esrally install --quiet --distribution-version=7.4.2 --node-name="rally-node-1" --network-host="192.168.14.78" --http-port=39200 --master-nodes="rally-node-0,rally-node-1,rally-node-2" --seed-hosts="192.168.14.77:39300,192.168.14.78:39300,192.168.14.79:39300" | jq --raw-output '.["installation-id"]')
    # on 192.168.14.79
    export INSTALLATION_ID=$(esrally install --quiet --distribution-version=7.4.2 --node-name="rally-node-2" --network-host="192.168.14.79" --http-port=39200 --master-nodes="rally-node-0,rally-node-1,rally-node-2" --seed-hosts="192.168.14.77:39300,192.168.14.78:39300,192.168.14.79:39300" | jq --raw-output '.["installation-id"]')

Then we pick a random race id, e.g. ``fb38013d-5d06-4b81-b81a-b61c8c10f6e5`` and set it on each machine (including the machine where will generate load)::

    export RACE_ID="fb38013d-5d06-4b81-b81a-b61c8c10f6e5"

Now we can start the cluster. Run the following command on each node::

    esrally start --installation-id="${INSTALLATION_ID}" --race-id="${RACE_ID}"

Once this has finished, we can check that the cluster is up and running e.g. with the ``_cat/health`` API::

    curl http://192.168.14.77:39200/_cat/health\?v

We should see that our cluster consisting of three nodes is up and running::

    epoch      timestamp cluster         status node.total node.data shards pri relo init unassign pending_tasks max_task_wait_time active_shards_percent
    1574930657 08:44:17  rally-benchmark green           3         3      0   0    0    0        0             0                  -                100.0%

Now we can start the benchmark on the load generator machine (remember to set the race id there)::

    esrally race --pipeline=benchmark-only --target-host=192.168.14.77:39200,192.168.14.78:39200,192.168.14.79:39200 --track=geonames --challenge=append-no-conflicts-index-only --on-error=abort --race-id=${RACE_ID}

Similarly to the single-node benchmark, we can now shutdown the cluster again by issuing the following command on each node::

    esrally stop --installation-id="${INSTALLATION_ID}"

