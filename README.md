# MIT-6.824 Distributed System

[course link](https://pdos.csail.mit.edu/6.824/index.html)

- [x] lab1 MapReduce
- [ ] lab2
- [ ] lab3
- [ ] lab4

## Lab1 MapReduce
All tests passed, no race
- [x] word count test
- [x] indexer test
- [x] map parallelism
- [x] reduce parallelism
- [x] job count test
- [x] early exit test
- [x] crash test

Files:

```
coordinator.go
worker.go
rpc.go
```

### Coordinator

Returns task info to workers

RPC calls seems to be concurrent, using a mutex to enforce sequential execution

### Worker

Ask coordinator for task, then map/reduce