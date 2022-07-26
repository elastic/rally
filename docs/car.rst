Configure Elasticsearch: Cars
=============================

.. note::

    You can skip this section if you use Rally only as a load generator.

Definition
----------

A Rally "car" is a specific configuration of Elasticsearch. You can list the available cars with ``esrally list cars``::

        ____        ____
       / __ \____ _/ / /_  __
      / /_/ / __ `/ / / / / /
     / _, _/ /_/ / / / /_/ /
    /_/ |_|\__,_/_/_/\__, /
                    /____/

    Name                     Type    Description
    -----------------------  ------  ----------------------------------
    16gheap                  car     Sets the Java heap to 16GB
    1gheap                   car     Sets the Java heap to 1GB
    2gheap                   car     Sets the Java heap to 2GB
    4gheap                   car     Sets the Java heap to 4GB
    8gheap                   car     Sets the Java heap to 8GB
    defaults                 car     Sets the Java heap to 1GB
    ea                       mixin   Enables Java assertions
    fp                       mixin   Preserves frame pointers
    x-pack-ml                mixin   X-Pack Machine Learning
    x-pack-monitoring-http   mixin   X-Pack Monitoring (HTTP exporter)
    x-pack-monitoring-local  mixin   X-Pack Monitoring (local exporter)
    x-pack-security          mixin   X-Pack Security

You can specify the car that Rally should use with e.g. ``--car="4gheap"``. It is also possible to specify one or more "mixins" to further customize the configuration. For example, you can specify ``--car="4gheap,ea"`` to run with a 4GB heap and enable Java assertions (they are disabled by default) or ``--car="4gheap,x-pack-security"`` to benchmark Elasticsearch with X-Pack Security enabled (requires Elasticsearch 6.3.0 or better).

.. note::
    To benchmark ``x-pack-security`` you need to add the following command line options: ``--client-options="use_ssl:true,verify_certs:false,basic_auth_user:'rally',basic_auth_password:'rally-password'"``


Similar to :doc:`custom tracks </adding_tracks>`, you can also define your own cars.

The Anatomy of a car
--------------------

The default car definitions of Rally are stored in ``~/.rally/benchmarks/teams/default/cars``. There we find the following structure::

    .
    └── v1
        ├── 1gheap.ini
        ├── 2gheap.ini
        ├── defaults.ini
        ├── ea
        │   └── templates
        │       └── config
        │           └── jvm.options
        ├── ea.ini
        └── vanilla
            ├── config.ini
            └── templates
                └── config
                    ├── elasticsearch.yml
                    ├── jvm.options
                    └── log4j2.properties

The top-level directory "v1" denotes the configuration format in version 1. Below that directory, each ``.ini`` file defines a car. Each directory (``ea`` or ``vanilla``) contains templates for the config files. Rally will only copy the files in the ``templates`` subdirectory. The top-level directory is reserved for a special file, ``config.ini`` which you can use to define default variables that apply to all cars that are based on this configuration. Below is an example ``config.ini`` file::

    [variables]
    clean_command=./gradlew clean

This defines the variable ``clean_command`` for all cars that reference this configuration. Rally will treat the following variable names specially:

* `clean_command`: The command to clean the Elasticsearch project directory.
* `build_command`: The command to build an Elasticsearch source distribution.
* `artifact_path_pattern`: A glob pattern to find a previously built source distribution within the project directory.
* `release_url`: A download URL for Elasticsearch distributions. The placeholder ``{{VERSION}}`` is replaced by Rally with the actual Elasticsearch version.

Let's have a look at the ``1gheap`` car by inspecting ``1gheap.ini``::

    [meta]
    description=Sets the Java heap to 1GB
    type=car

    [config]
    base=vanilla

    [variables]
    heap_size=1g

The name of the car is derived from the ``.ini`` file name. In the ``meta`` section we can provide a ``description`` and the ``type``. Use ``car`` if a configuration can be used standalone and ``mixin`` if it needs to be combined with other configurations. In the ``config`` section we define that this definition is based on the ``vanilla`` configuration. We also define a variable ``heap_size`` and set it to ``1g``. Note that variables defined here take precedence over variables defined in the ``config.ini`` file of any of the referenced configurations.

Let's open ``vanilla/config/templates/jvm.options`` to see how this variable is used (we'll only show the relevant part here)::

    # Xms represents the initial size of total heap space
    # Xmx represents the maximum size of total heap space

    -Xms{{heap_size}}
    -Xmx{{heap_size}}

So Rally reads all variables and the template files and replaces the variables in the final configuration. Note that Rally does not know anything about ``jvm.options`` or ``elasticsearch.yml``. For Rally, these are just plain text templates that need to be copied to the Elasticsearch directory before running a benchmark. Under the hood, Rally uses `Jinja2 <http://jinja.pocoo.org/docs/dev/>`_ as template language. This allows you to use Jinja2 expressions in your car configuration files.

If you open ``vanilla/templates/config/elasticsearch.yml`` you will see a few variables that are not defined in the ``.ini`` file:

* ``network_host``
* ``http_port``

These values are derived by Rally internally based on command line flags and you cannot override them in your car definition. You also cannot use these names as names for variables because Rally would simply override them.

If you specify multiple configurations, e.g. ``--car="4gheap,ea"``, Rally will apply them in order. It will first read all variables in ``4gheap.ini``, then in ``ea.ini``. Afterwards, it will copy all configuration files from the corresponding config base of ``4gheap`` and *append* all configuration files from ``ea``. This also shows when to define a separate "car" and when to define a "mixin": If you need to amend configuration files, use a mixin, if you need to have a specific configuration, define a car.

Simple customizations
^^^^^^^^^^^^^^^^^^^^^

For simple customizations you can create the directory hierarchy as outlined above and use the ``--team-path`` command line parameter to refer to this configuration. For more complex use cases and distributed multi-node benchmarks, we recommend to use custom team repositories.

Custom Team Repositories
^^^^^^^^^^^^^^^^^^^^^^^^

Rally provides a default team repository that is hosted on `Github <https://github.com/elastic/rally-teams>`_. You can also add your own team repositories although this requires a bit of additional work. First of all, team repositories need to be managed by git. The reason is that Rally can benchmark multiple versions of Elasticsearch and we use git branches in the track repository to determine the best match. The versioning scheme is as follows:

* The ``master`` branch needs to work with the latest ``main`` branch of Elasticsearch.
* All other branches need to match the version scheme of Elasticsearch, i.e. ``MAJOR.MINOR.PATCH-SUFFIX`` where all parts except ``MAJOR`` are optional.

Rally implements a branch matching logic similar to the one used for :ref:`track-repositories <track-repositories-branch-logic>`.

Creating a new team repository
""""""""""""""""""""""""""""""

All team repositories are located in ``~/.rally/benchmarks/teams``. If you want to add a dedicated team repository, called ``private`` follow these steps::

    cd ~/.rally/benchmarks/teams
    mkdir private
    cd private
    git init
    # add your team now (don't forget to add the subdirectory "cars").
    git add .
    git commit -m "Initial commit"


If you want to share your teams with others (or you want to run remote benchmarks) you need to add a remote and push it::

    git remote add origin git@git-repos.acme.com:acme/rally-teams.git
    git push -u origin master

If you have added a remote you should also add it in ``~/.rally/rally.ini``, otherwise you can skip this step. Open the file in your editor of choice and add the following line in the section ``teams``::

    private.url = <<URL_TO_YOUR_ORIGIN>>

Rally will then automatically update the local tracking branches before the benchmark starts.

.. warning::

    If you run benchmarks against a remote machine that is under the control of Rally then you need to add the custom team configuration on every node!


You can now verify that everything works by listing all teams in this team repository::

    esrally list cars --team-repository=private

This shows all teams that are available on the ``master`` branch of this repository. Suppose you only created tracks on the branch ``2`` because you're interested in the performance of Elasticsearch 2.x, then you can specify also the distribution version::

    esrally list teams --team-repository=private --distribution-version=7.0.0


Rally will follow the same branch fallback logic as described above.

Adding an already existing team repository
""""""""""""""""""""""""""""""""""""""""""

If you want to add a team repository that already exists, just open ``~/.rally/rally.ini`` in your editor of choice and add the following line in the section ``teams``::

    your_repo_name.url = <<URL_TO_YOUR_ORIGIN>>

After you have added this line, have Rally list the tracks in this repository::

    esrally list cars --team-repository=your_repo_name

