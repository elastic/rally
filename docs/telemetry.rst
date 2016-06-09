Telemetry Devices
=================

You probably want to gain additional insights from a race. Therefore, we have added telemetry devices to Rally. If you invoke
``esrally list telemetry``, it will show which telemetry devices are available::

    dm@io:~ $ esrally list telemetry
    
        ____        ____
       / __ \____ _/ / /_  __
      / /_/ / __ `/ / / / / /
     / _, _/ /_/ / / / /_/ /
    /_/ |_|\__,_/_/_/\__, /
                    /____/
    Available telemetry devices:

    Command    Name                   Description
    ---------  ---------------------  --------------------------------------------------------------------------------------
    jfr        Flight Recorder        Enables Java Flight Recorder on the benchmark candidate (will only work on Oracle JDK)
    jit        JIT Compiler Profiler  Enables JIT compiler logs.
    perf       perf stat              Reads CPU PMU counters (beta, only on Linux, requires perf)

    Keep in mind that each telemetry device may incur a runtime overhead which can skew results.

jfr
---

The ``jfr`` telemetry device enables the `Java Flight Recorder <http://docs.oracle.com/javacomponents/jmc-5-5/jfr-runtime-guide/index.html>`_ on the benchmark candidate. Java Flight Recorder ships only with the Oracle JDK, so Rally assumes that an Oracle JDK is used for benchmarking.

To enable ``jfr``, invoke Rally with ``esrally --telemetry jfr``. ``jfr`` will then write a flight recording file which can be opened in `Java Mission Control <http://www.oracle.com/technetwork/java/javaseproducts/mission-control/java-mission-control-1998576.html>`_. Rally prints the location of the flight recording file on the command line.
 
.. image:: jfr-es.png
   :alt: Sample Java Flight Recording

.. note::

   The licensing terms of Java flight recorder do not allow you to run it in production environments without a valid license (for details, please refer to the `Oracle Java SE Advanced & Suite Products page <http://www.oracle.com/technetwork/java/javaseproducts/overview/index.html>`_). However, running in a QA environment is fine.

jit
---

The ``jit`` telemetry device enables JIT compiler logs for the benchmark candidate. If the HotSpot disassembler library is available, the logs will also contain the disassembled JIT compiler output which can be used for low-level analysis. We recommend to use `JITWatch <https://github.com/AdoptOpenJDK/jitwatch>`_ for analysis.

The JITWatch wiki contains `build instructions for hsdis <https://github.com/AdoptOpenJDK/jitwatch/wiki/Building-hsdis>`_.

perf
----

The ``perf`` telemetry devices runs ``perf stat`` on each benchmarked node and writes the output to a log file. It can be used to capture low-level CPU statistics. Note that the perf tool, which is only available on Linux, must be installed before using this telemetry device.