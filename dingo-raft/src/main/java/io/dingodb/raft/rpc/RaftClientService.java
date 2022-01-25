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
import io.dingodb.raft.util.Endpoint;

import java.util.concurrent.Future;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public interface RaftClientService extends ClientService {
    /**
     * Sends a pre-vote request and handle the response with done.
     *
     * @param endpoint destination address (ip, port)
     * @param request  request data
     * @param done     callback
     * @return a future with result
     */
    Future<Message> preVote(final Endpoint endpoint, final RpcRequests.RequestVoteRequest request,
                            final RpcResponseClosure<RpcRequests.RequestVoteResponse> done);

    /**
     * Sends a request-vote request and handle the response with done.
     *
     * @param endpoint destination address (ip, port)
     * @param request  request data
     * @param done     callback
     * @return a future with result
     */
    Future<Message> requestVote(final Endpoint endpoint, final RpcRequests.RequestVoteRequest request,
                                final RpcResponseClosure<RpcRequests.RequestVoteResponse> done);

    /**
     * Sends a append-entries request and handle the response with done.
     *
     * @param endpoint destination address (ip, port)
     * @param request  request data
     * @param done     callback
     * @return a future with result
     */
    Future<Message> appendEntries(final Endpoint endpoint, final RpcRequests.AppendEntriesRequest request,
                                  final int timeoutMs, final RpcResponseClosure<RpcRequests.AppendEntriesResponse> done);

    /**
     * Sends a install-snapshot request and handle the response with done.
     *
     * @param endpoint destination address (ip, port)
     * @param request  request data
     * @param done     callback
     * @return a future result
     */
    Future<Message> installSnapshot(final Endpoint endpoint, final RpcRequests.InstallSnapshotRequest request,
                                    final RpcResponseClosure<RpcRequests.InstallSnapshotResponse> done);

    /**
     * Get a piece of file data by GetFileRequest, and handle the response with done.
     *
     * @param endpoint  destination address (ip, port)
     * @param request   request data
     * @param timeoutMs timeout millis
     * @param done      callback
     * @return a future result
     */
    Future<Message> getFile(final Endpoint endpoint, final RpcRequests.GetFileRequest request, final int timeoutMs,
                            final RpcResponseClosure<RpcRequests.GetFileResponse> done);

    /**
     * Send a timeout-now request and handle the response with done.
     *
     * @param endpoint  destination address (ip, port)
     * @param request   request data
     * @param timeoutMs timeout millis
     * @param done      callback
     * @return a future result
     */
    Future<Message> timeoutNow(final Endpoint endpoint, final RpcRequests.TimeoutNowRequest request,
                               final int timeoutMs, final RpcResponseClosure<RpcRequests.TimeoutNowResponse> done);

    /**
     * Send a read-index request and handle the response with done.
     *
     * @param endpoint  destination address (ip, port)
     * @param request   request data
     * @param timeoutMs timeout millis
     * @param done      callback
     * @return a future result
     */
    Future<Message> readIndex(final Endpoint endpoint, final RpcRequests.ReadIndexRequest request, final int timeoutMs,
                              final RpcResponseClosure<RpcRequests.ReadIndexResponse> done);
}
