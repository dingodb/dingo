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

package io.dingodb.coordinator.handler;

import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import io.dingodb.common.util.StackTraces;
import io.dingodb.coordinator.meta.MetaAdaptor;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.MessageListener;
import io.dingodb.store.row.cmd.pd.GetStoreIdRequest;
import io.dingodb.store.row.cmd.pd.GetStoreIdResponse;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GetStoreIdHandler implements MessageListener, RpcProcessor<GetStoreIdRequest> {

    public static final Class<GetStoreIdRequest> REQ_CLASS = GetStoreIdRequest.class;

    private final MetaAdaptor metaAdaptor;

    public GetStoreIdHandler(MetaAdaptor metaAdaptor) {
        this.metaAdaptor = metaAdaptor;
    }

    @Override
    public void onMessage(Message message, Channel channel) {

    }

    @Override
    public void handleRequest(RpcContext rpcCtx, GetStoreIdRequest request) {
        log.info("{}", StackTraces.stack());
        GetStoreIdResponse response = new GetStoreIdResponse();
        response.setValue(metaAdaptor.storeId(request.getEndpoint()));
        rpcCtx.sendResponse(response);
    }

    @Override
    public String interest() {
        return REQ_CLASS.getName();
    }
}
