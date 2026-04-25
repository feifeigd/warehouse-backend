# distributed-nodes

A tree-shaped distributed service sample built on top of `caf::io`.

Each node type now has its own entry file and executable instead of sharing one
`main.cpp`.

## Roles

- `master`: the single management node
- `region`: branch node below the master
- `compute`: leaf node for analytics work
- `storage`: leaf node for key lookup
- `client`: demo caller

## Topology

```text
master
`-- region-a
    |-- compute-a1
    `-- storage-a1
```

## Remote lookup model

Each server node opens a CAF middleman port and exports one or more named
actors via `sys.registry().put(name, actor)`.

- `master.control`
- `region.router`
- `compute.service`
- `storage.service`
- `node.control`

Callers use:

1. `middleman.connect(host, port)`
2. `middleman.remote_lookup(actor_name, node_id)`
3. regular actor messaging on the returned handle

The sample uses `remote_lookup` in three places:

- node bootstrap: children locate `master.control`
- tree attachment: leaves locate `region.router`
- client requests: the client looks up master, region, compute, and storage
  actors directly

## Build

```powershell
cmake --build --preset windows-x64 --target distributed-nodes
```

## Run

One-click start for all server nodes in dedicated PowerShell windows:

```powershell
.\distributed-nodes\scripts\start-nodes.cmd
```

One-click stop for all server nodes:

```powershell
.\distributed-nodes\scripts\stop-nodes.cmd
```

One-click start for the demo client in a dedicated PowerShell window:

```powershell
.\distributed-nodes\scripts\start-client.cmd
```

One-click full demo run: start nodes, run client, then stop nodes automatically:

```powershell
.\distributed-nodes\scripts\run-demo.cmd
```

Start the master:

```powershell
.\out\build\windows-x64\distributed-nodes\Debug\distributed-nodes-master.exe --config-file distributed-nodes\master.conf
```

Start the region and leaf nodes:

```powershell
.\out\build\windows-x64\distributed-nodes\Debug\distributed-nodes-region.exe --config-file distributed-nodes\region-a.conf
.\out\build\windows-x64\distributed-nodes\Debug\distributed-nodes-compute.exe --config-file distributed-nodes\compute-a1.conf
.\out\build\windows-x64\distributed-nodes\Debug\distributed-nodes-storage.exe --config-file distributed-nodes\storage-a1.conf
```

Call through the topology from a client:

```powershell
.\out\build\windows-x64\distributed-nodes\Debug\distributed-nodes-client.exe --config-file distributed-nodes\client.conf
```

## Config files

- `caf-application.conf`: default debugger config
- `master.conf`: root management node
- `region-a.conf`: branch node under the master
- `compute-a1.conf`: compute leaf under `region-a`
- `storage-a1.conf`: storage leaf under `region-a`
- `client.conf`: demo client that traverses the tree

## Entry Files

- `master_main.cpp`
- `region_main.cpp`
- `compute_main.cpp`
- `storage_main.cpp`
- `client_main.cpp`
- `app.hpp`: shared types, actor logic, and helper functions
