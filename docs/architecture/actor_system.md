# Rally Actor System

At its heart, Rally is a distributed system. It has been designed that way to
allow using both multiple target hosts and load drivers in the same benchmark.
The latter ensures that Rally is never a bottleneck. In the vast majority of
cases, using a powerful load driver is enough, but benchmarking large
Elasticsearch clusters containing tens or hundreds of nodes can require more
load drivers.

## Thespian

Actors are managed by [Thespian](https://thespianpy.com/doc/) which provide us
with the following features:

 * Works with Linux and macOS (and Windows, though Rally does not support Windows)
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

 * The mechanic actors that can set up an Elasticsearch cluster are not covered
 * Failure and cancellation are ignored, this is about the happy path
 * This pretends that we are benchmarking on a single machine with a single
   core.

The sequence diagram below starts with `BenchmarkActor` defined in
`racecontrol.py` which does the high-level scheduling:

 * Setup the Elasticsearch cluster if the pipeline requires it (not covered
   here)
 * Prepare the benchmark which involves the `TrackPreparationActor` and
   `TaskExecutionActor` actors.
 * Start the benchmark, which will involve the `Worker` actor that will
   delegate the actual work to `AsyncIoAdapter`, which will run an asyncio loop
   in a thread, which is how `AsyncExecutor` runs many Elasticsearch async
   clients.

You'll notice that `DriverActor` and `Driver` are tightly coupled. While
`Driver` contains most of the logic, it is not an actor, so it relies on
`DriverActor` to send and receive messages. This was done mainly to enable
`Driver` unit testing without bringing the actor system up.

### PrepareBenchmark

Here's the sequence diagram for benchmark preparation. Dotted lines
mean messages sent with the actor systems, while plain lines are
function calls.

```mermaid
sequenceDiagram
    BenchmarkActor ->> BenchmarkCoordinator: __init__
    BenchmarkActor ->> BenchmarkCoordinator: setup
    BenchmarkActor ->> DriverActor: __init__
    BenchmarkActor -->> DriverActor: PrepareBenchmark
    DriverActor ->> Driver: __init__
    DriverActor ->> Driver: prepare_benchmark
    Driver ->> DriverActor: prepare_track
    DriverActor ->> TrackPreparationActor: __init__
    DriverActor -->> TrackPreparationActor: Bootstrap
    TrackPreparationActor -->> DriverActor: ReadyForWork
    DriverActor -->> TrackPreparationActor: PrepareTrack
    TrackPreparationActor ->> TaskExecutionActor: __init__
    TrackPreparationActor -->> TaskExecutionActor: StartTaskLoop
    loop
        TaskExecutionActor -->> TrackPreparationActor: ReadyForWork
        TrackPreparationActor -->> TaskExecutionActor: DoTask
        loop
            TaskExecutionActor -->> TaskExecutionActor: WakeupMessage
        end
    end
    TaskExecutionActor -->> TrackPreparationActor: WorkerIdle
    TrackPreparationActor -->> DriverActor: TrackPrepared
    DriverActor -->> TrackPreparationActor: ActorExitRequest
    TrackPreparationActor -->> TaskExecutionActor: ActorExitRequest
    Driver -->> BenchmarkActor: PreparationComplete
    BenchmarkActor ->> BenchmarkCoordinator: on_preparation_complete
```

### StartBenchmark

Once the preparation is complete, `BenchmarkActor` starts the
benchmark. As above, dotted lines means messages sent with the actor
systems, while plain lines are function calls.

Note: The `WakeupMessage` loop in `DriverActor` denoted with `(*)` continues in
parallel until all steps of the benchmark are done but it is kept short in the
diagram for clarity.

```mermaid
sequenceDiagram
    participant BenchmarkActor
    participant BenchmarkCoordinator
    BenchmarkActor -->> DriverActor: StartBenchmark
    DriverActor ->> Driver: start_benchmark
    loop
        DriverActor -->> DriverActor: WakeupMessage (*)
        DriverActor ->> Driver: post_process_samples
        DriverActor ->> Driver: update_progress_messages
    end
    Driver ->> DriverActor: create_client
    DriverActor ->> Worker: __init__
    DriverActor -->> Worker: Bootstrap
    Driver ->> DriverActor: start_worker
    DriverActor -->> Worker: StartWorker
    loop
        DriverActor -->> Worker: Drive
        loop
            Worker ->> Worker: drive
            Worker ->> AsyncIoAdapter: __init__
            AsyncIoAdapter ->> Worker: "thread finished"
            Worker -->> Worker: WakeupMessage
            Worker -->> DriverActor: UpdateSamples
            DriverActor ->> Driver: update_samples
        end
        Worker -->> DriverActor: UpdateSamples
        DriverActor ->> Driver: update_samples
        Worker -->> DriverActor: JoinPointReached
        DriverActor ->> Driver: joinpoint_reached
        Driver ->> DriverActor: on_task_finished
        DriverActor -->> BenchmarkActor: TaskFinished
        BenchmarkActor ->> BenchmarkCoordinator: on_task_finished
    end
    Driver ->> DriverActor: on_benchmark_complete
    DriverActor -->> BenchmarkActor: BenchmarkComplete
    BenchmarkActor ->> BenchmarkCoordinator: on_benchmark_complete
```
