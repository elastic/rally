Pipelines
=========

A pipeline is a series of steps that are performed to get benchmark results. This is *not* intended to customize the actual benchmark but rather what happens before and after a benchmark.

An example will clarify the concept: If you want to benchmark a binary distribution of Elasticsearch, Rally has to download a distribution archive, decompress it, start Elasticsearch and then run the benchmark. However, if you want to benchmark a source build of Elasticsearch, it first has to build a distribution with Gradle. So, in both cases, different steps are involved and that's what pipelines are for.

You can get a list of all pipelines with ``esrally list pipelines``::

    Available pipelines:

    Name                     Description
    -----------------------  ---------------------------------------------------------------------------------------------
    from-distribution        Downloads an Elasticsearch distribution, provisions it, runs a benchmark and reports results.
    from-sources-complete    Builds and provisions Elasticsearch, runs a benchmark and reports results.
    benchmark-only           Assumes an already running Elasticsearch instance, runs a benchmark and reports results
    from-sources-skip-build  Provisions Elasticsearch (skips the build), runs a benchmark and reports results.

benchmark-only
~~~~~~~~~~~~~~

This is intended if you want to provision a cluster by yourself. Do not use this pipeline unless you are absolutely sure you need to. As Rally has not provisioned the cluster, results are not easily reproducable and it also cannot gather a lot of metrics (like CPU usage). We will eventually allow to set up multi-node clusters at some point though so this pipeline will become obsolete over time. It is likely that we remove it then.

To benchmark a cluster, you also have to specify the hosts to connect to. An example invocation::

    esrally --pipeline=benchmark-only --target-hosts=search-node-a.intranet.acme.com:9200,search-node-b.intranet.acme.com:9200


from-distribution
~~~~~~~~~~~~~~~~~

This pipeline allows to benchmark an official Elasticsearch distribution which will be automatically downloaded by Rally. The earliest supported version is Elasticsearch 5.0.0-alpha1. An example invocation::

    esrally --pipeline=from-distribution --distribution-version=5.0.0-alpha1

The version numbers have to match the name in the download URL path.

You can also benchmark Elasticsearch snapshot versions by specifying the snapshot repository::

    esrally --pipeline=from-distribution --distribution-version=5.0.0-SNAPSHOT --distribution-repository=snapshot

However, this feature is mainly intended for continuous integration environments and by default you should just benchmark official distributions.

from-sources-complete
~~~~~~~~~~~~~~~~~~~~~

You should use this pipeline when you want to build and benchmark Elasticsearch from sources. Remember that you also need to install git and Gradle before and Rally needs to be configured for building for sources. If that's not the case you'll get an error and have to run ``esrally configure`` first. An example invocation::

    esrally --pipeline=from-sources-complete --revision=latest

You have to specify a :ref:`revision </command_line_reference>`.

from-sources-skip-build
~~~~~~~~~~~~~~~~~~~~~~~

This pipeline is similar to ``from-sources-complete`` except that it assumes you have built the binary once. It saves time if you want to run a benchmark twice for the exact same version of Elasticsearch. Obviously it doesn't make sense to provide a revision: It is always the previously built revision. An example invocation::

    esrally --pipeline=from-sources-skip-build

