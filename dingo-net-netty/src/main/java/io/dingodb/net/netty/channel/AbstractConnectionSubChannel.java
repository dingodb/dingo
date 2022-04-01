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

package io.dingodb.net.netty.channel;

import io.dingodb.net.netty.connection.Connection;
import io.dingodb.net.netty.packet.Packet;
import io.dingodb.net.netty.utils.Logs;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public abstract class AbstractConnectionSubChannel<M> implements Runnable, ConnectionSubChannel<M> {

    private static final ThreadGroup THREAD_GROUP = new ThreadGroup("Connection channel");

    protected final ChannelId channelId;
    protected final Connection<M> connection;
    private final AtomicLong seqNo;
    protected ChannelId targetChannelId;

    protected AbstractConnectionSubChannel(
        ChannelId channelId,
        ChannelId targetChannelId,
        Connection<M> connection
    ) {
        this.channelId = channelId;
        this.targetChannelId = targetChannelId;
        this.connection = connection;
        this.seqNo = new AtomicLong(1);
    }

    @Override
    public ChannelId channelId() {
        return channelId;
    }

    @Override
    public ChannelId targetChannelId() {
        return targetChannelId;
    }

    @Override
    public void targetChannelId(ChannelId channelId) {
        this.targetChannelId = channelId;
    }

    @Override
    public void close() {

    }

    @Override
    public Connection<M> connection() {
        return connection;
    }

    @Override
    public void send(Packet<M> packet) {
        Logs.packetDbg(true, log, connection, packet);
        connection.send(packet);
    }

    @Override
    public long nextSeq() {
        return seqNo.getAndIncrement();
    }
}
