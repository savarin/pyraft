# pyraft

pyraft implements the Raft distributed consensus algorithm.

## Quickstart

The Raft cluster consists of 3 servers. To start each server, run the respective command in a new terminal window. A numbered prompt indicates the server is running. For server 1:

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

The color of the prompt indicates the current role of the server - red for follower, yellow for candidate and green for leader.

To start the client, run the following command in a new terminal window.

```shell
> python src/raftclient.py 0

0 >
```

The client is used to send instructions to the server. The `append` command instructs the server to append entries to its log, with the command prefixed by the server number. For example, to instruct server 1 to append entries `a`, `b` and `c` to its log:

```shell
0 > 1 append a b c
```

To instruct all servers to expose its state:

```shell
0 > self
```

*This project was completed as a part of David Beazleys's [Rafting Trip](https://www.dabeaz.com/raft.html) class.*
