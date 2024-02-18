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

package io.dingodb.net.netty;

import io.dingodb.common.Location;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.concurrent.LinkedRunner;
import io.dingodb.common.util.NoBreakFunctions;
import io.dingodb.net.netty.api.ApiRegistryImpl;
import io.dingodb.net.netty.api.AuthProxyApi;
import io.dingodb.net.netty.api.HandshakeApi;
import io.dingodb.net.netty.api.HandshakeApi.Handshake;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.AttributeMap;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static io.dingodb.common.codec.PrimitiveCodec.readString;
import static io.dingodb.common.util.DebugLog.debug;
import static io.dingodb.net.netty.Constant.API_T;
import static io.dingodb.net.netty.Constant.AUTH;
import static io.dingodb.net.netty.Constant.CLIENT;
import static io.dingodb.net.netty.Constant.COMMAND_T;
import static io.dingodb.net.netty.Constant.HANDSHAKE;
import static io.dingodb.net.netty.Constant.PING_C;
import static io.dingodb.net.netty.Constant.S2C;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
@Accessors(fluent = true)
public class Connection {

    private final Map<Long, Channel> channels = new ConcurrentHashMap<>();
    private final AtomicLong channelIdSeq = new AtomicLong(0);
    private final String channelType;
    private final long direction;
    @Getter
    private final Channel channel;
    @Getter
    private final Location remote;
    @Getter
    @Delegate(excludes = {ChannelOutboundInvoker.class, AttributeMap.class})
    private final SocketChannel socket;
    private final List<Consumer<Connection>> closeListeners = new ArrayList<>();
    @Getter
    private Map<String, Object[]> authContent;

    private ScheduledFuture<?> heartbeatFuture;

    public Connection(String chanelType, Location remote, SocketChannel socket) {
        this.channelType = chanelType;
        this.socket = socket;
        this.remote = remote;
        this.channel = createChannel(0);
        this.channels.put(0L, channel);
        this.direction = CLIENT.equals(chanelType) ? Constant.C2S : S2C;
    }

    protected Channel getChannel(long channelId) {
        Channel channel = channels.get(channelId);
        if (channel == null) {
            // if current connection direction is server to client, always create client to server channel
            // if current connection direction is client to server, always create server to client channel
            if (direction == S2C) {
                if ((S2C & channelId) != S2C) {
                    channel = createChannel(channelId);
                }
            } else {
                if ((S2C & channelId) == S2C) {
                    channel = createChannel(channelId);
                }
            }
        }
        return channel;
    }

    private void removeChannel(long channelId) {
        channels.remove(channelId);
        debug(log, "Removed channel {} to remote \"{}\". # of channels: {}", channelId, remote.url(), channels.size());
    }

    protected Channel createChannel(long channelId) {
        debug(log, "Create channel {} to remote \"{}\". # of channels: {}", channelId, remote.url(), channels.size());

        return channels.computeIfAbsent(
            channelId,
            id -> new Channel(channelId, this, new LinkedRunner(fmtName(remote.url(), channelId)), this::removeChannel)
        );
    }

    private String fmtName(String url, long id) {
        StringBuilder builder = new StringBuilder("<");
        builder.append(url).append("/").append(id).append("/").append(channelType).append(">");
        return builder.toString();
    }

    private void sendHeartbeat() {
        channel.sendAsync(channel.buffer(COMMAND_T, 1).writeByte(PING_C));
    }

    public void handshake() {
        ApiRegistryImpl.instance().proxy(HandshakeApi.class, channel).handshake(null, Handshake.INSTANCE);
        log.info("Connection handshake success, remote: [{}]", remote.url());
    }

    public void handshake(ByteBuffer message) {
        if (message.getLong() != 0 || message.get() != API_T || !HANDSHAKE.equals(readString(message))) {
            log.error("Illegal connection [{}].", remote.url());
            close();
            return;
        }
        ApiRegistryImpl.instance().invoke(HANDSHAKE, channel, message);
    }

    public void auth() {
        authContent = AuthProxyApi.auth(channel);
        log.info("Connection auth success, remote: [{}]", remote.url());
        heartbeatFuture = Executors.scheduleWithFixedDelayAsync(
            String.format("%s-heartbeat", remote.url()), this::sendHeartbeat, 0, 1, SECONDS
        );
    }

    public void auth(ByteBuffer message) {
        if (message.getLong() != 0 || message.get() != API_T || !AUTH.equals(readString(message))) {
            log.error("Illegal connection [{}].", remote.url());
            close();
            return;
        }
        authContent = ApiRegistryImpl.instance().invoke(AUTH, channel, message);
    }

    public void receive(ByteBuffer message) {
        if (message == null) {
            return;
        }
        long channelId = message.getLong();
        Channel channel = getChannel(channelId);
        if (channel == null) {
            log.error("Receive message, channel id is [{}], but not have channel.", channelId);
            return;
        }
        channel.receive(message);
    }

    public void send(ByteBuf message) throws InterruptedException {
        if (channel.isClosed()) {
            throw new RuntimeException("Connection closed.");
        }
        socket.writeAndFlush(message).await();
    }

    public void sendAsync(ByteBuf message) {
        if (channel.isClosed()) {
            throw new RuntimeException("Connection closed.");
        }
        socket.writeAndFlush(message);
    }

    public Channel newChannel() {
        return createChannel(channelIdSeq.incrementAndGet() | direction);
    }

    public synchronized void addCloseListener(Consumer<Connection> consumer) {
        closeListeners.add(consumer);
    }

    public void close() {
        debug(log, "Close connection to [{}].", remote);
        if (heartbeatFuture != null) {
            heartbeatFuture.cancel(true);
        }
        channel.shutdown();
        channels.values().forEach(Channel::shutdown);
        channels.clear();
        if (authContent != null) {
            authContent.clear();
        }
        if (socket.isActive()) {
            socket.disconnect();
        }
        closeListeners.forEach(NoBreakFunctions.wrap(listener -> {
            listener.accept(this);
        }));
        debug(log, "Closed connection to [{}].", remote);
    }

}
