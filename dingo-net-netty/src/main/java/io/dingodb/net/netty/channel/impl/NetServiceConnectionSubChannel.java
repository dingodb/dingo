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

package io.dingodb.net.netty.channel.impl;

import io.dingodb.common.error.CommonError;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.MessageListener;
import io.dingodb.net.NetAddress;
import io.dingodb.net.Tag;
import io.dingodb.net.netty.api.ApiRegistryImpl;
import io.dingodb.net.netty.channel.AbstractConnectionSubChannel;
import io.dingodb.net.netty.channel.ChannelId;
import io.dingodb.net.netty.connection.Connection;
import io.dingodb.net.netty.handler.impl.TagMessageHandler;
import io.dingodb.net.netty.packet.Packet;
import io.dingodb.net.netty.packet.PacketMode;
import io.dingodb.net.netty.packet.PacketType;
import io.dingodb.net.netty.packet.impl.MessagePacket;
import io.dingodb.net.netty.utils.Logs;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Slf4j
public class NetServiceConnectionSubChannel extends AbstractConnectionSubChannel<Message> implements Channel {

    private static final ThreadGroup THREAD_GROUP = new ThreadGroup("NetServiceConnectionSubChannel");

    private Status status;
    private MessageListener listener;
    private Consumer<Channel> closeListener;
    private NetAddress localAddress;
    private NetAddress remoteAddress;
    private Connection.ChannelPool channelPool;

    private final ApiRegistryImpl apiRegistry = ApiRegistryImpl.instance();
    private final BlockingQueue<Packet<Message>> packetQueue = new LinkedBlockingQueue<>();

    public NetServiceConnectionSubChannel(
        ChannelId channelId,
        ChannelId targetChannelId,
        Connection<Message> connection
    ) {
        super(channelId, targetChannelId, connection);
        listener = this::skipListener;
        closeListener = this::skipListener;
        status = Status.NEW;
    }

    @Override
    public void send(Message sendMsg) {
        MessagePacket packet = MessagePacket.builder()
            .msgNo(nextSeq())
            .mode(PacketMode.USER_DEFINE)
            .type(PacketType.USER_DEFINE)
            .channelId(channelId)
            .targetChannelId(targetChannelId)
            .content(sendMsg)
            .build();
        send(packet);
    }

    @Override
    public void run() {
        Packet<Message> packet = null;
        while ((status != Status.CLOSE && status != Status.WAIT) || !packetQueue.isEmpty()) {
            try {
                if ((packet = packetQueue.poll(1, TimeUnit.SECONDS)) == null) {
                    continue;
                }
                if (log.isDebugEnabled()) {
                    Logs.packetDbg(false, log, connection, packet);
                }
                if (packet.header().mode() == PacketMode.API && packet.header().type() == PacketType.INVOKE) {
                    apiRegistry.invoke(this, (MessagePacket) packet);
                } else {
                    if (listener != null) {
                        listener.onMessage(packet.content(), this);
                    }
                    Tag tag = packet.content().tag();
                    if (tag != null) {
                        TagMessageHandler.instance().handler(this, tag, packet);
                    }
                }
            } catch (InterruptedException e) {
                CommonError.EXEC_INTERRUPT.throwFormatError("channel consume packet", Thread.currentThread(), "--");
            } catch (Exception e) {
                Logs.packetErr(false, log, connection, packet , e.getMessage(), e);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("Channel {}, {} finish", status, Thread.currentThread().getName());
        }
    }

    private void skipListener(Message message, Channel channel) {
    }

    private void skipListener(Channel channel) {
    }

    @Override
    public void setChannelPool(Connection.ChannelPool pool) {
        this.channelPool = pool;
    }

    @Override
    public void registerMessageListener(MessageListener listener) {
        this.listener = listener;
    }

    @Override
    public void closeListener(Consumer<Channel> listener) {
        if (listener == null) {
            this.closeListener = this::skipListener;
        } else {
            this.closeListener = listener;
        }
    }

    public NetServiceConnectionSubChannel status(Status status) {
        this.status = status;
        return this;
    }

    @Override
    public Status status() {
        return status;
    }

    @Override
    public NetAddress localAddress() {
        return localAddress = new NetAddress(connection.localAddress());
    }

    @Override
    public NetAddress remoteAddress() {
        return remoteAddress = new NetAddress(connection.remoteAddress());
    }

    @Override
    public void receive(Packet<Message> packet) {
        try {
            packetQueue.put(packet);
        } catch (InterruptedException e) {
            CommonError.EXEC_INTERRUPT.throwFormatError("channel receive packet", Thread.currentThread(), "--");
        }
    }

    @Override
    public synchronized void start() {
        status = Status.ACTIVE;
    }

    @Override
    public void close() {
        if (channelPool != null) {
            status = Status.WAIT;
            channelPool.offer(this);
            listener = this::skipListener;
            closeListener = this::skipListener;
            if (log.isDebugEnabled()) {
                log.debug("Channel [{}/{}] ----> [{}/{}] queue recycling",
                    localAddress(), channelId, remoteAddress(), targetChannelId);
            }
        } else {
            if (status == Status.ACTIVE) {
                send(MessagePacket.disconnectRemoteChannel(channelId, targetChannelId(), nextSeq()));
            }
            status = Status.CLOSE;
            closeListener.accept(this);
            connection.closeSubChannel(channelId);
            if (log.isDebugEnabled()) {
                log.debug(
                    "Channel [{}/{}] ---> [{}/{}] close.",
                    localAddress(), channelId, remoteAddress(), targetChannelId
                );
            }
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        NetServiceConnectionSubChannel that = (NetServiceConnectionSubChannel) other;
        return Objects.equals(channelId, that.channelId)
            && Objects.equals(remoteAddress, that.remoteAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(channelId, remoteAddress);
    }
}
