# raft

raft implements the Raft distributed consensus algorithm.

## Quickstart

The Raft cluster consists of 3 servers. To start each server, run the following commands in three separate terminal windows. A numbered prompt indicates the server is running, with the current role of the server by prompt color - red for follower, yellow for candidate and green for leader.

For server 1:

```shell
> python src/raftserver.py 1

1 >
```

For server 2:

```shell
> python src/raftserver.py 2

2 >
```

For server 3:

```shell
> python src/raftserver.py 3

3 >
```

To start the client, run the following command in a new terminal window.

```shell
> python src/raftclient.py 0

0 >
```

The `append` command instructs the server to append entries to its log. The command needs to be prefixed by the server number. For example, to instruct server 1 to append entries `a`, `b` and `c` to its log:

```shell
0 > 1 append a b c
```

To instruct all servers to expose its state:

```shell
0 > self
```

*This project was completed as a part of David Beazley's [Rafting Trip](https://www.dabeaz.com/raft.html) class.*
