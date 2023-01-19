# MIT-6.824 Distributed System

[course link](https://pdos.csail.mit.edu/6.824/index.html)

### Tests Passed:
- [x] Lab1: MapReduce
- [x] Lab2A: Raft, leader election
- [x] Lab2B: Raft, log
- [x] Lab2C: Raft, persistence
- [x] Lab2D: Raft, log compaction
- [ ] Lab3A: Fault-tolerant Key/Value Service without snapshots
- [ ] Lab3B: Fault-tolerant Key/Value Service with snapshots
- [ ] Lab4: Sharded Key/Value Service



### Run Tests

Lab1

```
cd src/main
bash test-mr.sh 
```

Lab2

```
cd src/raft
go test -race
```



