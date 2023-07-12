# Rally Actor System

At its heart, Rally is a distributed system. It has been designed that way to
allow using multiple load drivers in the same benchmark, to ensure that Rally
is never a bottleneck. In the vast majority of cases, using a powerful load
driver is enough, but benchmarks large Elasticsearch clusters containing tens
or hundreds of nodes can require more load drivers.

## Thespian

Actors are managed by [Thespian](https://thespianpy.com/doc/) which provide us
with the following features:

 * Works with Linux and macOS (and Windows, but Rally does not need that)
 * Handles the communication between actors regardless of their locations
 * Scales from running Rally and Elasticsearch on one workstation to
   benchmarking large Elasticsearch clusters with multiple load drivers,
   without any change to the Rally codebase.

While it is not without its rough edges, it is [well
documented](https://thespianpy.com/doc/using.html) and battle tested.
Additionally, the maintainer has always been responsive and helpful.

## Sequence diagrams

Rally has a number of actors that all inherit from `actor.RallyActor`. This
document focuses on the actors needed to *prepare* and *run* a benchmark, with
the following limitations:

 * The mechanic actors that can setup an Elasticsearch cluster are not covered
 * Failure and cancellation are ignored, this is about the happy path
 * This pretends that we are benchmarking on a single machine with a single
   core.

The sequence diagram below starts with `BenchmarkActor` defined in
`racecontrol.py` which does the high-level scheduling:

 * Setup the Elasticsearch cluster if the pipeline requires it (not covered
   here)
 * Prepare the benchmark which involves the `TrackPreparator` and
   `TaskExecutionActor` actors.
 * Start the benchmark, which will involve the `Worker` actor that will
   delegate the actual work to `AsyncIoAdapter`, that will run an asyncio loop
   in a thread, which is how `AsyncExecutor` runs many Elasticsearch async
   clients.

You'll notice that `DriverActor` and `Driver` are tightly coupled. While
`Driver` contains most of the logic, it is not an actor, so it relies on
`DriverActor` to send and receive messages. This was done for two reasons:

 * `Driver` can be unit-tested without bringing the actor system
 * An earlier attempt at removing the actor system failed.

### PrepareBenchmark

Here's the sequence diagram for benchmark preparation. Dotted lines
means messages sent with the actor systems, while plain lines are
function calls.

```mermaid
sequenceDiagram
    BenchmarkActor ->> DriverActor: __init__
    BenchmarkActor -->> DriverActor: PrepareBenchmark
    DriverActor ->> Driver: __init__
    DriverActor ->> Driver: prepare_benchmark
    Driver ->> DriverActor: prepare_track
    DriverActor ->> TrackPreparator: __init__
    DriverActor -->> TrackPreparator: Bootstrap
    TrackPreparator -->> DriverActor: ReadyForWork
    DriverActor -->> TrackPreparator: PrepareTrack
    TrackPreparator ->> TaskExecutionActor: __init__
    TrackPreparator -->> TaskExecutionActor: StartTaskLoop
    loop
        TaskExecutionActor -->> TrackPreparator: ReadyForWork
        TrackPreparator -->> TaskExecutionActor: DoTask
        loop
            TaskExecutionActor -->> TaskExecutionActor: WakeupMessage
        end
    end
    TaskExecutionActor -->> TrackPreparator: WorkerIdle
    TrackPreparator -->> DriverActor: TrackPrepared
    DriverActor -->> TrackPreparator: ActorExitRequest
    TrackPreparator -->> TaskExecutionActor: ActorExitRequest
    Driver -->> BenchmarkActor: PreparationComplete
```

### StartBenchmark

Once the preparation is complete, `BenchmarkActor` starts the
benchmark. As above, dotted lines means messages sent with the actor
systems, while plain lines are function calls.

```mermaid
sequenceDiagram
    BenchmarkActor -->> DriverActor: StartBenchmark
    loop
        DriverActor -->> DriverActor: WakeupMessage
        DriverActor ->> Driver: post_process_samples
        DriverActor ->> Driver: update_progress_messages
    end
    DriverActor ->> Driver: start_benchmark
    Driver ->> DriverActor: create_client
    DriverActor ->> Worker: __init__
    Driver ->> DriverActor: start_worker
    DriverActor -->> Worker: StartWorker
    loop
        loop
            Worker ->> AsyncIoAdapter: __init__
            AsyncIoAdapter ->> Worker: "thread finished"
            Worker -->> Worker: WakeupMessage
        end
        Worker -->> DriverActor: JoinPointReached
        DriverActor ->> Driver: joinpoint_reached
    end
    Driver ->> DriverActor: on_benchmark_complete
    DriverActor -->> BenchmarkActor: BenchmarkComplete
```
