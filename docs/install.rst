Installation
------------

This is the detailed installation guide for Rally. If you are in a hurry you can check the :doc:`quickstart guide </quickstart>`.

Prerequisites
~~~~~~~~~~~~~

Before installing Rally, please ensure that the following packages are installed:

* Python 3.4 or better available as `python3` on the path (verify with: ``python3 --version`` which should print ``Python 3.4.0`` or higher)
* ``pip3`` available on the path (verify with ``pip3 --version``)
* JDK 8
* git 1.9 or better

Rally does not support Windows and is only actively tested on Mac OS X and Linux.

.. note::

   If you use RHEL, please ensure to install a recent version of git via the `Red Hat Software Collections <https://www.softwarecollections.org/en/scls/rhscl/git19/>`_.


Installing Rally
~~~~~~~~~~~~~~~~

Simply install Rally with pip: ``pip3 install esrally``

.. note::

   Depending on your system setup you may need to prepend this command with ``sudo``.

If you get errors during installation, it is probably due to the installation of ``psutil`` which we use to gather system metrics like CPU utilization. Please check the `installation instructions of psutil <https://github.com/giampaolo/psutil/blob/master/INSTALL.rst>`_ in this case. Keep in mind that Rally is based on Python 3 and you need to install the Python 3 header files instead of the Python 2 header files on Linux.

Non-sudo Install
~~~~~~~~~~~~~~~~

If you don't want to use ``sudo`` when installing Rally, installation is still possible but a little more involved:

1. Specify the ``--user`` option when installing Rally (step 2 above), so the command to be issued is: ``python3 setup.py develop --user``.
2. Check the output of the install script or lookup the `Python documentation on the variable site.USER_BASE <https://docs.python.org/3.5/library/site.html#site.USER_BASE>`_ to find out where the script is located. On Linux, this is typically ``~/.local/bin``.

You can now either add ``~/.local/bin`` to your path or invoke Rally via ``~/.local/bin/esrally`` instead of just ``esrally``.

VirtualEnv Install
~~~~~~~~~~~~~~~~~~

You can also use Virtualenv to install Rally into an isolated Python environment without sudo.

1. Set up a new virtualenv environment in a directory with ``virtualenv --python=python3 .``
2. Activate the environment with ``source /path/to/virtualenv/dir/bin/activate``
3. Install Rally with ``pip install esrally``

Whenever you want to use Rally, run the activation script (step 2 above) first.  When you are done, simply execute ``deactivate`` in the shell to exit the virtual environment.


Next Steps
~~~~~~~~~~

After you have installed, you need to configure it. Just run ``esrally configure`` or follow the :doc:`configuration help page </configuration>` for more guidance.
