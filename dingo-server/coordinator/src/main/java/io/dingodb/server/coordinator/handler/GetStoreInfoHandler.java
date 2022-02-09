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
import io.dingodb.store.row.cmd.pd.GetStoreInfoRequest;
import io.dingodb.store.row.cmd.pd.GetStoreInfoResponse;
import io.dingodb.store.row.errors.Errors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GetStoreInfoHandler implements MessageListener, RpcProcessor<GetStoreInfoRequest> {

    public static final Class<GetStoreInfoRequest> REQ_CLASS = GetStoreInfoRequest.class;

    private final RowStoreMetaAdaptor rowStoreMetaAdaptor;

    public GetStoreInfoHandler(RowStoreMetaAdaptor rowStoreMetaAdaptor) {
        this.rowStoreMetaAdaptor = rowStoreMetaAdaptor;
    }

    @Override
    public void onMessage(Message message, Channel channel) {

    }

    @Override
    public void handleRequest(RpcContext rpcCtx, GetStoreInfoRequest request) {
        GetStoreInfoResponse response = new GetStoreInfoResponse();
        try {
            if (rowStoreMetaAdaptor.available()) {
                response.setValue(rowStoreMetaAdaptor.storeInfo(request.getEndpoint()));
            } else {
                response.setError(Errors.NOT_LEADER);
            }
            rpcCtx.sendResponse(response);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public String interest() {
        return REQ_CLASS.getName();
    }

}
