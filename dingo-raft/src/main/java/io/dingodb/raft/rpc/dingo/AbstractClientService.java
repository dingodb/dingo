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

package io.dingodb.raft.rpc.dingo;

import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.dingodb.common.Location;
import io.dingodb.net.Channel;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.raft.Status;
import io.dingodb.raft.error.RaftError;
import io.dingodb.raft.option.RpcOptions;
import io.dingodb.raft.rpc.ClientService;
import io.dingodb.raft.rpc.ProtobufMsgFactory;
import io.dingodb.raft.rpc.RpcClient;
import io.dingodb.raft.rpc.RpcRequests;
import io.dingodb.raft.rpc.RpcResponseClosure;
import io.dingodb.raft.rpc.RpcResponseFactory;
import io.dingodb.raft.rpc.impl.FutureImpl;
import io.dingodb.raft.util.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ServiceLoader;
import java.util.concurrent.Future;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public abstract class AbstractClientService implements ClientService {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractClientService.class);

    static {
        ProtobufMsgFactory.load();
    }

    NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    protected volatile RpcClient rpcClient;
    protected RpcOptions rpcOptions;

    public RpcClient getRpcClient() {
        return this.rpcClient;
    }

    @Override
    public boolean checkConnection(final Endpoint endpoint, final boolean createIfAbsent) {
        Location remote = new Location(endpoint.getIp(), endpoint.getPort());
        Channel channel = null;
        try {
            channel = netService.newChannel(remote);
        } catch (Exception e) {
            return false;
        } finally {
            if (channel != null) {
                try {
                    channel.close();
                } catch (Exception e) {
                    LOG.error("CheckConnection Error: {}", e.getMessage());
                }
            }
        }
        return true;
    }

    @Override
    public synchronized boolean init(final RpcOptions rpcOptions) {
        return true;
    }

    @Override
    public synchronized void shutdown() {

    }

    @Override
    public <T extends Message> Future<Message> invokeWithDone(Message result, RpcResponseClosure<T> done,
                                                              FutureImpl<Message> future, Channel channel) {
        Status status = Status.OK();
        Message msg = result;
        if (result instanceof RpcRequests.ErrorResponse) {
            status = handleErrorResponse((RpcRequests.ErrorResponse) result);
        } else {
            try {
                final Descriptors.FieldDescriptor fd = result.getDescriptorForType() //
                    .findFieldByNumber(RpcResponseFactory.ERROR_RESPONSE_NUM);
                if (fd != null && result.hasField(fd)) {
                    final RpcRequests.ErrorResponse eResp = (RpcRequests.ErrorResponse) result.getField(fd);
                    status = handleErrorResponse(eResp);
                    msg = eResp;
                }
            } catch (Exception e) {
                status = new Status();
                status.setCode(RaftError.ENOENT.getNumber());
            }
        }
        if (done != null) {
            try {
                if (status.isOk()) {
                    done.setResponse((T) msg);
                }
                done.run(status);
            } catch (final Throwable t) {
                LOG.error("Fail to run RpcResponseClosure", t);
            }
        }
        future.setResult(msg);
        try {
            channel.close();
        } catch (Exception e) {
            future.failure(e);
        }
        return future;
    }

    public <T extends Message> Future<Message> invokeWithDone(
        Endpoint endpoint, String tag, Message request, RpcResponseClosure<T> done, ThrowableFunction<byte[], T> parser
    ) {
        Location remote = new Location(endpoint.getIp(), endpoint.getPort());
        io.dingodb.net.Message message = new io.dingodb.net.Message(tag, request.toByteArray());
        Channel channel = null;
        try {
            channel = netService.newChannel(remote, true);
            FutureImpl<Message> future = new FutureImpl<>(channel);
            channel.setMessageListener((msg, ch) -> {
                Message result = null;
                try {
                    result = parser.apply(msg.content());
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Recive Message {} : {}", ch.remoteLocation(), result.toString());
                    }
                } catch (InvalidProtocolBufferException e) {
                    try {
                        result = RpcRequests.ErrorResponse.parseFrom(msg.content());
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Recive ErrorMessage {} : {}", ch.remoteLocation(), result.toString());
                        }
                    } catch (InvalidProtocolBufferException ex) {
                        try {
                            ch.close();
                        } catch (Exception exc) {
                            throw new RuntimeException(exc);
                        }
                        LOG.error("Error Msg from {} : {}", ch.remoteLocation(), msg.content());
                    }
                }
                invokeWithDone(result, done, future, ch);
            });
            if (LOG.isDebugEnabled()) {
                LOG.debug("Send Msg {} : {}", channel.remoteLocation(), message.content().toString());
            }
            channel.send(message);
            return future;
        } catch (Exception e) {
            if (done != null) {
                done.run(new Status(RaftError.ETIMEDOUT, "RPC exception:" + e.getMessage()));
            }
            FutureImpl<Message> failFuture = new FutureImpl<>(channel);
            failFuture.failure(e);
            return failFuture;
        }
    }

    private static Status handleErrorResponse(final RpcRequests.ErrorResponse errRes) {
        final Status status = new Status();
        status.setCode(errRes.getErrorCode());
        if (errRes.hasErrorMsg()) {
            status.setErrorMsg(errRes.getErrorMsg());
        }
        return status;
    }
}
