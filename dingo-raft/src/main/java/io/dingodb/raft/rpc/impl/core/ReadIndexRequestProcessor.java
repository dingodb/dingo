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

package io.dingodb.raft.rpc.impl.core;

import com.google.protobuf.Message;
import io.dingodb.raft.Status;
import io.dingodb.raft.rpc.RaftServerService;
import io.dingodb.raft.rpc.RpcRequestClosure;
import io.dingodb.raft.rpc.RpcRequests;
import io.dingodb.raft.rpc.RpcResponseClosureAdapter;

import java.util.concurrent.Executor;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class ReadIndexRequestProcessor extends NodeRequestProcessor<RpcRequests.ReadIndexRequest> {
    public ReadIndexRequestProcessor(Executor executor) {
        super(executor, RpcRequests.ReadIndexResponse.getDefaultInstance());
    }

    @Override
    protected String getPeerId(final RpcRequests.ReadIndexRequest request) {
        return request.getPeerId();
    }

    @Override
    protected String getGroupId(final RpcRequests.ReadIndexRequest request) {
        return request.getGroupId();
    }

    @Override
    public Message processRequest0(final RaftServerService service, final RpcRequests.ReadIndexRequest request,
                                   final RpcRequestClosure done) {
        service.handleReadIndexRequest(request, new RpcResponseClosureAdapter<RpcRequests.ReadIndexResponse>() {

            @Override
            public void run(final Status status) {
                if (getResponse() != null) {
                    done.sendResponse(getResponse());
                } else {
                    done.run(status);
                }
            }

        });
        return null;
    }

    @Override
    public String interest() {
        return RpcRequests.ReadIndexRequest.class.getName();
    }
}
