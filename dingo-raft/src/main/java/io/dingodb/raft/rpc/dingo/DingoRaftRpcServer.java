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

import com.google.protobuf.Message;
import io.dingodb.net.Channel;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.net.SimpleMessage;
import io.dingodb.net.Tag;
import io.dingodb.raft.rpc.Connection;
import io.dingodb.raft.rpc.RpcContext;
import io.dingodb.raft.rpc.RpcProcessor;
import io.dingodb.raft.rpc.RpcServer;
import io.dingodb.raft.rpc.impl.ConnectionClosedEventListener;

import java.util.ServiceLoader;

public class DingoRaftRpcServer implements RpcServer {

    NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    private int port;

    @Override
    public boolean init(Void opts) {
        return true;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public void registerConnectionClosedEventListener(ConnectionClosedEventListener listener) {

    }

    @Override
    public void registerProcessor(RpcProcessor processor) {
        netService.registerTagMessageListener(processor.getRequestTag(), (msg, ch) -> {
            this.port = ch.localAddress().port();
            RpcContext rpcCtx = new DingoRpcContext(processor.getResponseTag(), ch);
            processor.handleRequest(rpcCtx, processor.parse(msg.toBytes()));
        });
    }

    @Override
    public int boundPort() {
        return this.port;
    }
};

class DingoRpcContext implements RpcContext {

    Tag tag;
    Channel channel;

    public DingoRpcContext(Tag tag, Channel changel) {
        this.tag = tag;
        this.channel = changel;
    }

    @Override
    public void sendResponse(Object responseObj) {
        channel.send(SimpleMessage.builder()//.tag(this.tag)
            .content(((Message)responseObj).toByteArray()).build());
    }

    @Override
    public Connection getConnection() {
        return new Connection() {
            @Override
            public Object getAttribute(String key) {
                return null;
            }

            @Override
            public void setAttribute(String key, Object value) {

            }

            @Override
            public Object setAttributeIfAbsent(String key, Object value) {
                return null;
            }

            @Override
            public void close() {
                try {
                    channel.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    public String getRemoteAddress() {
        return channel.remoteAddress().toString();
    }
}
