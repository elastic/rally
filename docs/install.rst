Installation
============

This is the detailed installation guide for Rally. If you are in a hurry you can check the :doc:`quickstart guide </quickstart>`.

Prerequisites
-------------

Rally does not support Windows and is only actively tested on MacOS and Linux. Before installing Rally, please ensure that the following packages are installed.

Python
~~~~~~

* Python 3.4 or better available as `python3` on the path. Verify with: ``python3 --version``.
* Python3 header files (included in the Python3 development package).
* ``pip3`` available on the path. Verify with ``pip3 --version``.

**Debian / Ubuntu**

::

    sudo apt-get install gcc python3-pip python3-dev


**RHEL 6/ CentOS 6**

*Tested on CentOS release 6.9 (Final).*

.. note::

    Depending on the concrete flavor and version of your distribution you may need to enable `EPEL <https://fedoraproject.org/wiki/EPEL>`_ before.

::

    sudo yum install -y gcc python34.x86_64 python34-devel.x86_64 python34-setuptools.noarch
    # installs pip as it is not available as an OS package
    sudo python3 /usr/lib/python3.4/site-packages/easy_install.py pip


**RHEL 7 / CentOS 7**

*Tested on CentOS Linux release 7.4.1708 (Core).*

::

    sudo yum install -y gcc python34.x86_64 python34-devel.x86_64 python34-pip.noarch

**Amazon Linux**

::

    sudo yum install -y gcc python35-pip.noarch python35-devel.x86_64

**MacOS**

We recommend that you use `Homebrew <https://brew.sh/>`_::

    brew install python3

git
~~~

``git 1.9`` or better is required. Verify with ``git --version``.

**Debian / Ubuntu**

::

    sudo apt-get install git


**Red Hat / CentOS / Amazon Linux**

::

    sudo yum install git


.. note::

   If you use RHEL, please ensure to install a recent version of git via the `Red Hat Software Collections <https://www.softwarecollections.org/en/scls/rhscl/git19/>`_.

**MacOS**

``git`` is already installed on MacOS.

JDK
~~~

A JDK is required on all machines where you want to launch Elasticsearch. If you use Rally just as a load generator, no JDK is required.

We recommend to use Oracle JDK but you are free to use OpenJDK as well. For details on how to install a JDK, please see your Linux distribution's documentation pages.

Installing Rally
----------------

Simply install Rally with pip: ``pip3 install esrally``

.. note::

   Depending on your system setup you may need to prepend this command with ``sudo``.

If you get errors during installation, it is probably due to the installation of ``psutil`` which we use to gather system metrics like CPU utilization. Please check the `installation instructions of psutil <https://github.com/giampaolo/psutil/blob/master/INSTALL.rst>`_ in this case. Keep in mind that Rally is based on Python 3 and you need to install the Python 3 header files instead of the Python 2 header files on Linux.

Non-sudo Install
----------------

If you don't want to use ``sudo`` when installing Rally, installation is still possible but a little more involved:

1. Specify the ``--user`` option when installing Rally (step 2 above), so the command to be issued is: ``python3 setup.py develop --user``.
2. Check the output of the install script or lookup the `Python documentation on the variable site.USER_BASE <https://docs.python.org/3.5/library/site.html#site.USER_BASE>`_ to find out where the script is located. On Linux, this is typically ``~/.local/bin``.

You can now either add ``~/.local/bin`` to your path or invoke Rally via ``~/.local/bin/esrally`` instead of just ``esrally``.

VirtualEnv Install
------------------

You can also use Virtualenv to install Rally into an isolated Python environment without sudo.

1. Set up a new virtualenv environment in a directory with ``virtualenv --python=python3 .``
2. Activate the environment with ``source /path/to/virtualenv/dir/bin/activate``
3. Install Rally with ``pip install esrally``

Whenever you want to use Rally, run the activation script (step 2 above) first.  When you are done, simply execute ``deactivate`` in the shell to exit the virtual environment.


Next Steps
----------

After you have installed, you need to configure it. Just run ``esrally configure`` or follow the :doc:`configuration help page </configuration>` for more guidance.
