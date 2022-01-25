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

package io.dingodb.raft.rpc;

import com.google.protobuf.Message;
import io.dingodb.raft.rpc.RpcRequests.AppendEntriesRequest;
import io.dingodb.raft.rpc.RpcRequests.InstallSnapshotRequest;
import io.dingodb.raft.rpc.RpcRequests.ReadIndexRequest;
import io.dingodb.raft.rpc.RpcRequests.ReadIndexResponse;
import io.dingodb.raft.rpc.RpcRequests.RequestVoteRequest;
import io.dingodb.raft.rpc.RpcRequests.TimeoutNowRequest;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public interface RaftServerService {
    /**
     * Handle pre-vote request.
     *
     * @param request   data of the pre vote
     * @return the response message
     */
    Message handlePreVoteRequest(RequestVoteRequest request);

    /**
     * Handle request-vote request.
     *
     * @param request   data of the vote
     * @return the response message
     */
    Message handleRequestVoteRequest(RequestVoteRequest request);

    /**
     * Handle append-entries request, return response message or
     * called done.run() with response.
     *
     * @param request   data of the entries to append
     * @param done      callback
     * @return the response message
     */
    Message handleAppendEntriesRequest(AppendEntriesRequest request, RpcRequestClosure done);

    /**
     * Handle install-snapshot request, return response message or
     * called done.run() with response.
     *
     * @param request   data of the install snapshot request
     * @param done      callback
     * @return the response message
     */
    Message handleInstallSnapshot(InstallSnapshotRequest request, RpcRequestClosure done);

    /**
     * Handle time-out-now request, return response message or
     * called done.run() with response.
     *
     * @param request   data of the timeout now request
     * @param done      callback
     * @return the response message
     */
    Message handleTimeoutNowRequest(TimeoutNowRequest request, RpcRequestClosure done);

    /**
     * Handle read-index request, call the RPC closure with response.
     *
     * @param request   data of the readIndex read
     * @param done      callback
     */
    void handleReadIndexRequest(ReadIndexRequest request, RpcResponseClosure<ReadIndexResponse> done);
}
