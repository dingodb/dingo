/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.server.coordinator.api;

import io.dingodb.common.Location;
import io.dingodb.net.NetService;
import io.dingodb.raft.Node;
import io.dingodb.raft.entity.PeerId;
import io.dingodb.raft.error.RemotingException;
import io.dingodb.raft.rpc.RpcClient;
import io.dingodb.raft.rpc.impl.cli.GetLocationProcessor;

import java.util.ArrayList;
import java.util.List;

public class CoordinatorServerApi implements io.dingodb.server.api.CoordinatorServerApi {

    private final Node node;
    private final RpcClient rpcClient;

    public CoordinatorServerApi(Node node, RpcClient rpcClient, NetService netService) {
        this.node = node;
        this.rpcClient = rpcClient;
        netService.apiRegistry().register(io.dingodb.server.api.CoordinatorServerApi.class, this);
    }

    @Override
    public Location leader() {
        try {
            return ((GetLocationProcessor.GetLocationResponse) rpcClient.invokeSync(
                node.getLeaderId().getEndpoint(),
                GetLocationProcessor.GetLocationRequest.INSTANCE,
                3000
            )).getLocation();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Location> getAll() {
        List<PeerId> peerIds = node.listPeers();
        List<Location> locations = new ArrayList<>();
        for (PeerId peerId : peerIds) {
            try {
                locations.add(((GetLocationProcessor.GetLocationResponse) rpcClient.invokeSync(
                    peerId.getEndpoint(),
                    GetLocationProcessor.GetLocationRequest.INSTANCE,
                    3000
                )).getLocation());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (RemotingException e) {
                throw new RuntimeException(e);
            }
        }
        return locations;
    }
}
