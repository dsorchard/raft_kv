## RAFT KV

### Reference
- [Raft  KV Example](https://github.com/otoolep/hraftd)

### Run
```shell
go build
```

```shell
./raft_kv -id node0 ~/node0
```

```shell
./raft_kv -id node1 -haddr 127.0.0.1:11001 -raddr 127.0.0.1:12001 -join 127.0.0.1:11000 ~/node1
```

```shell
./raft_kv -id node2 -haddr 127.0.0.1:11002 -raddr 127.0.0.1:12002 -join 127.0.0.1:11000 ~/node2
```

```shell
## Make sure you hit the master node
curl -XPOST localhost:11000/key -d '{"user1": "alex"}'
```

```shell
## Get can happen on any node
curl -XGET localhost:11002/key/user1
```
