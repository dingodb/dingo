#  Snapshot

The store node triggers a snapshot.

## Manually trigger a snapshot

```shell
bin/dingodb_client_store --method=Snapshot --addrs=The corresponding business port for the store (non-Raft port) is --region_id=1007 (corresponding to the ID of the region)
```

## Automatically trigger a snapshot

```shell
dingo@inspur201 ➜  dist git:(main) ✗ cat store1/conf/store.yaml
cluster:
  name: dingodb
  instance_id: 1001
  coordinators: 192.168.1.201:22001,192.168.1.201:22002,192.168.1.201:22003
  keyring: TO_BE_CONTINUED
server:
  host: 192.168.1.201
  port: 20001
  heartbeatInterval: 10000 # ms
  metricsCollectInterval: 300000 # ms
raft:
  host: 192.168.1.201
  port: 20101
  path: /home/dingo/huzx/dingo-store/dist/store1/data/store/raft
  electionTimeout: 1000 # ms
  snapshotInterval: 60 # This identifies the time interval for Snapshot departure。
```

## View Snapshot

### Mode one:using the browser function provided in Brpc

Access the browser address： http://ip:20101/raft_stat

```
2_XYZ_part_0_1007
peer_id: 192.168.1.201:20103:0
state: LEADER
readonly: 0
term: 2
conf_index: 4115622
peers: 192.168.1.201:20101:0 192.168.1.201:20102:0 192.168.1.201:20103:0
changing_conf: NO stage: STAGE_NONE
election_timer: timeout(1000ms) STOPPED
vote_timer: timeout(2000ms) STOPPED
stepdown_timer: timeout(1000ms) SCHEDULING(in 484ms)
snapshot_timer: timeout(600000ms) SCHEDULING(in 282096ms)
storage: [2036336, 5216082]
disk_index: 5216082
known_applied_index: 5216081
last_log_id: (index=5216082,term=2)
state_machine: Idle
last_committed_index: 5216081
pending_index: 5216082
pending_queue_size: 1
last_snapshot_index: 4115622
last_snapshot_term: 2
snapshot_status: IDLE
replicator_14353780703353@192.168.1.201:20101:0: next_index=5216083 flying_append_entries_size=1 appending [5216082, 5216082] hc=15168 ac=5215677 ic=0
replicator_27565100105857@192.168.1.201:20102:0: next_index=5216083 flying_append_entries_size=1 appending [5216082, 5216082] hc=15168 ac=5215659 ic=0
```

### Mode two

```
View the directory corresponding to the disk，./store1/data/store/raft/1001/snapshot/snapshot_xxx.
Note: Only one snapshot operation can be performed at a time in a store, and there is only one snapshot directory.
```
