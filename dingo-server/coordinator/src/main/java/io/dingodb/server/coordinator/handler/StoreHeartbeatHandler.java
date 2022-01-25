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

package io.dingodb.server.coordinator.handler;

import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.MessageListener;
import io.dingodb.raft.rpc.RpcContext;
import io.dingodb.raft.rpc.RpcProcessor;
import io.dingodb.server.coordinator.meta.RowStoreMetaAdaptor;
import io.dingodb.store.row.cmd.pd.StoreHeartbeatRequest;
import io.dingodb.store.row.cmd.pd.StoreHeartbeatResponse;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StoreHeartbeatHandler implements MessageListener, RpcProcessor<StoreHeartbeatRequest> {

    public static final Class<StoreHeartbeatRequest> REQ_CLASS = StoreHeartbeatRequest.class;

    private final RowStoreMetaAdaptor rowStoreMetaAdaptor;

    public StoreHeartbeatHandler(RowStoreMetaAdaptor rowStoreMetaAdaptor) {
        this.rowStoreMetaAdaptor = rowStoreMetaAdaptor;
    }

    @Override
    public void onMessage(Message message, Channel channel) {
    }

    @Override
    public void handleRequest(RpcContext rpcCtx, StoreHeartbeatRequest request) {
        rowStoreMetaAdaptor.saveStoreStats(request.getStats());
        rpcCtx.sendResponse(new StoreHeartbeatResponse());
    }

    @Override
    public String interest() {
        return REQ_CLASS.getName();
    }

}
