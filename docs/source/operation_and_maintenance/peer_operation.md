# Peer Operation

## ADDPeer
The parameter --id specifies the store_id to which you want to expand, while --region-id specifies the region number you want to expand.

```shell
./dingodb_client_coordinator --method=AddPeerRegion --id=33004 --region-id=77001
After execution, you can confirm the Raft status of the region by using the QueryRegion method to view the region information.
./dingodb_client_coordinator --method=QueryRegion --id=77001
```
## RemovePeer
The parameter --id specifies the store_id to be removed from the region, while --region-id specifies the region number to be scaled down.

```shell
./dingodb_client_coordinator --method=RemovePeerRegion --id=33002 --region-id=77001
```
