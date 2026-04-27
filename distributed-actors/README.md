# distributed-actors-codex

A small distributed cluster sample built on top of `caf::io`.

It uses three runtime roles in a single executable:

- `seed`: publishes a coordinator actor with `caf::io::middleman`
- `worker`: connects to the coordinator and registers a local worker actor
- `client`: submits demo jobs to the coordinator and prints the results

## Topology

```text
client --> seed/coordinator --> worker-1
                           \-> worker-2
                           \-> worker-N
```

The coordinator keeps:

- a worker registry
- a FIFO job queue
- round-robin dispatch across idle workers
- simple failover by requeueing in-flight work when a worker disconnects

## Build

From the repository root:

```powershell
cmake --build --preset windows-x64 --target distributed-actors-codex
```

## Run

When you launch from `distributed-actors-codex` in the debugger, CAF will
auto-load [caf-application.conf](G:/git/warehouse-backend/distributed-actors-codex/caf-application.conf). From the repository root, pass an explicit config file:

Start the seed node:

```powershell
.\out\build\windows-x64\distributed-actors-codex\Debug\distributed-actors-codex.exe --config-file distributed-actors-codex\seed.conf
```

For scripted runs, give the node a fixed lifetime instead of waiting for
`<enter>`:

```powershell
.\out\build\windows-x64\distributed-actors-codex\Debug\distributed-actors-codex.exe --config-file distributed-actors-codex\seed.conf --lifetime 30
```

Start one or more workers in other terminals:

```powershell
.\out\build\windows-x64\distributed-actors-codex\Debug\distributed-actors-codex.exe --config-file distributed-actors-codex\worker-a.conf
.\out\build\windows-x64\distributed-actors-codex\Debug\distributed-actors-codex.exe --config-file distributed-actors-codex\worker-b.conf
```

Submit demo jobs from a client:

```powershell
.\out\build\windows-x64\distributed-actors-codex\Debug\distributed-actors-codex.exe --config-file distributed-actors-codex\client.conf
```

The shipped config files are:

- [caf-application.conf](G:/git/warehouse-backend/distributed-actors-codex/caf-application.conf): default local debug config
- [seed.conf](G:/git/warehouse-backend/distributed-actors-codex/seed.conf): coordinator node
- [worker-a.conf](G:/git/warehouse-backend/distributed-actors-codex/worker-a.conf): first worker node
- [worker-b.conf](G:/git/warehouse-backend/distributed-actors-codex/worker-b.conf): second worker node
- [client.conf](G:/git/warehouse-backend/distributed-actors-codex/client.conf): demo client

## Messages

- `worker_join_atom`: worker registration
- `job_submit_atom`: client submits a job
- `cluster_stats_atom`: client fetches cluster metrics
- `run_job_atom`: coordinator dispatches work to a worker

## Demo workloads

- `sum`: sum a vector of integers
- `max`: maximum value in a vector
- `dot`: dot product of two vectors
