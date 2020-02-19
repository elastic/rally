Offline Usage
=============

In some corporate environments servers do not have Internet access. You can still use Rally in such environments and this page summarizes all information that you need to get started.

Installation and Configuration
------------------------------

We provide a special offline installation package. Follow the :ref:`offline installation guide <install_offline-install>` and :doc:`configure Rally as usual </configuration>` afterwards.

Command Line Usage
------------------

Rally will automatically detect upon startup that no Internet connection is available and print the following warning::

    [WARNING] No Internet connection detected. Automatic download of track data sets etc. is disabled.

It detects this by trying to connect to ``https://github.com``. If you want it to probe against a different HTTP endpoint (e.g. a company-internal git server) you need to add a configuration property named ``probing.url`` in the ``system`` section of Rally's configuration file at ``~/.rally/rally.ini``. Specify ``--offline`` if you want to disable probing entirely.

Example of ``system`` section with custom probing url in ``~/.rally/rally.ini``::

    [system]
    env.name = local
    probing.url = https://www.company-internal-server.com/


Using tracks
------------

A Rally track describes a benchmarking scenario. You can either write your own tracks or use the tracks that Rally provides out of the box. In the former case, Rally will work just fine in an offline environment. In the latter case, Rally would normally download the track and its associated data from the Internet. If you want to use one of Rally's standard tracks in offline mode, you need to download all relevant files first on a machine that has Internet access and copy it to the target machine(s).

Use the `download script <https://raw.githubusercontent.com/elastic/rally-tracks/master/download.sh>`_ to download all data for a track on a machine that has access to the Internet. Example::

    # downloads the script from Github
    curl -O https://raw.githubusercontent.com/elastic/rally-tracks/master/download.sh
    chmod u+x download.sh
    # download all data for the geonames track
    ./download.sh geonames

This will download all data for the geonames track and create a tar file ``rally-track-data-geonames.tar`` in the current directory. Copy this file to the home directory of the user which will execute Rally on the target machine (e.g. ``/home/rally-user``).

On the target machine, run::

    cd ~
    tar -xf rally-track-data-geonames.tar

The download script does not require a Rally installation on the machine with Internet access but assumes that ``git`` and ``curl`` are available.

After you've copied the data, you can list the available tracks with ``esrally list tracks``. If a track shows up in this list, it just means that the track description is available locally but not necessarily all data files.

Using cars
----------

.. note::

    You can skip this section if you use Rally only as a load generator.

If you have Rally configure and start Elasticsearch then you also need the out-of-the-box configurations available. Run the following command on a machine with Internet access::

    git clone https://github.com/elastic/rally-teams.git ~/.rally/benchmarks/teams/default
    tar -C ~ -czf rally-teams.tar.gz .rally/benchmarks/teams/default

Copy that file to the target machine(s) and run on the target machine::

    cd ~
    tar -xzf rally-teams.tar.gz

After you've copied the data, you can list the available tracks with ``esrally list cars``.
