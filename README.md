# MIT-6.824 Distributed System

[course link](https://pdos.csail.mit.edu/6.824/index.html)

- [x] lab1 MapReduce
- [ ] lab2
- [ ] lab3
- [ ] lab4

## Lab1 MapReduce

All tests passed, no race

```
clover@DESKTOP-1MPCHVJ:~/distributed_system/6.824/src/main$ bash test-mr.sh 
*** Starting wc test.
--- wc test: PASS
*** Starting indexer test.
--- indexer test: PASS
*** Starting map parallelism test.
--- map parallelism test: PASS
*** Starting reduce parallelism test.
--- reduce parallelism test: PASS
*** Starting job count test.
--- job count test: PASS
*** Starting early exit test.
--- early exit test: PASS
*** Starting crash test.
--- crash test: PASS
*** PASSED ALL TESTS
```

Related files:

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
