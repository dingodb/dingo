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
import io.dingodb.net.service.AuthService;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static io.dingodb.common.codec.PrimitiveCodec.readString;
import static io.dingodb.net.netty.Constant.API_T;
import static io.dingodb.net.netty.Constant.AUTH;
import static io.dingodb.net.netty.Constant.COMMAND_T;
import static io.dingodb.net.netty.Constant.HANDSHAKE;
import static io.dingodb.net.netty.Constant.PING_C;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
@Accessors(fluent = true)
public class Connection  {

    private final Map<Long, Channel> channels = new ConcurrentHashMap<>();
    private final AtomicLong channelIdSeq = new AtomicLong(0);
    private final String channelFmt;
    private final boolean alwaysCreate;
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

    public Connection(String chanelFmt, Location remote, SocketChannel socket, boolean alwaysCreate) {
        this.channelFmt = chanelFmt;
        this.socket = socket;
        this.alwaysCreate = alwaysCreate;
        this.remote = remote;
        this.channel = createChannel(0);
        this.channels.put(0L, channel);
    }

    protected Channel getChannel(long channelId) {
        Channel channel = channels.get(channelId);
        if (channel == null && alwaysCreate) {
            channel = createChannel(channelId);
        }
        return channel;
    }

    protected Channel createChannel(long channelId) {
        return channels.computeIfAbsent(
            channelId,
            id -> new Channel(channelId, this, new LinkedRunner(fmtName(remote.getUrl(), channelId)), channels::remove)
        );
    }

    private String fmtName(String url, long id) {
        return String.format(channelFmt, url, id);
    }

    private void sendHeartbeat() {
        channel.sendAsync(channel.buffer(COMMAND_T, 1).writeByte(PING_C));
    }

    public void handshake() {
        ApiRegistryImpl.instance().proxy(HandshakeApi.class, channel).handshake(null, Handshake.INSTANCE);
        log.info("Connection open, remote: [{}]", remote.getUrl());
    }

    public void handshake(ByteBuffer message) {
        if (message.getLong() != 0 || message.get() != API_T || !HANDSHAKE.equals(readString(message))) {
            log.error("Illegal connection [{}].", remote.getUrl());
            close();
            return;
        }
        ApiRegistryImpl.instance().invoke(HANDSHAKE, channel, message);
    }

    public void auth() {
        Map<String, Object> certificates = new HashMap<>();
        for (AuthService.Provider authServiceProvider : AuthProxyApi.serviceProviders) {
            AuthService<Object> authService = authServiceProvider.get();
            certificates.put(authService.tag(), authService.createCertificate());
        }
        ApiRegistryImpl.instance().proxy(AuthProxyApi.class, channel).auth(null, certificates);
        log.info("Connection auth success, remote: [{}]", remote.getUrl());
        heartbeatFuture = Executors.scheduleWithFixedDelayAsync(
            String.format("%s-heartbeat", remote.getUrl()), this::sendHeartbeat, 0, 1, SECONDS
        );
    }

    public void auth(ByteBuffer message) {
        if (message.getLong() != 0 || message.get() != API_T || !AUTH.equals(readString(message))) {
            log.error("Illegal connection [{}].", remote.getUrl());
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
        socket.writeAndFlush(message).await();
    }

    public void sendAsync(ByteBuf message) {
        socket.writeAndFlush(message);
    }

    public Channel newChannel() {
        return createChannel(channelIdSeq.incrementAndGet());
    }

    public synchronized void addCloseListener(Consumer<Connection> consumer) {
        closeListeners.add(consumer);
    }

    public void close() {
        if (heartbeatFuture != null) {
            heartbeatFuture.cancel(true);
        }
        channel.shutdown();
        channels.values().forEach(Channel::shutdown);
        channels.clear();
        if (socket.isActive()) {
            socket.disconnect();
        }
        closeListeners.forEach(NoBreakFunctions.wrap(listener -> {
            listener.accept(this);
        }));
    }

}
