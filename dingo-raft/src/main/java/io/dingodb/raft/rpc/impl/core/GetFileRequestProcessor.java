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
import io.dingodb.raft.rpc.RpcRequestClosure;
import io.dingodb.raft.rpc.RpcRequestProcessor;
import io.dingodb.raft.rpc.RpcRequests;
import io.dingodb.raft.storage.FileService;

import java.util.concurrent.Executor;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class GetFileRequestProcessor extends RpcRequestProcessor<RpcRequests.GetFileRequest> {
    public GetFileRequestProcessor(Executor executor) {
        super(executor, RpcRequests.GetFileResponse.getDefaultInstance());
    }

    @Override
    public Message processRequest(final RpcRequests.GetFileRequest request, final RpcRequestClosure done) {
        return FileService.getInstance().handleGetFile(request, done);
    }

    @Override
    public String interest() {
        return RpcRequests.GetFileRequest.class.getName();
    }
}
