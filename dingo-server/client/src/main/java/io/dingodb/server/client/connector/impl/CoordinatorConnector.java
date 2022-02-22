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

package io.dingodb.server.client.connector.impl;

import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.concurrent.ThreadPoolBuilder;
import io.dingodb.common.util.PreParameters;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.NetAddress;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.server.client.connector.Connector;
import io.dingodb.server.protocol.Tags;
import io.dingodb.server.protocol.code.RaftServiceCode;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static io.dingodb.server.protocol.code.BaseCode.PING;
import static io.dingodb.server.protocol.code.RaftServiceCode.GET_ALL_LOCATION;
import static io.dingodb.server.protocol.code.RaftServiceCode.GET_LEADER_LOCATION;

@Slf4j
public class CoordinatorConnector implements Connector {

    private static final ExecutorService executorService = new ThreadPoolBuilder().name("CoordinatorConnector").build();

    private final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();
    private final AtomicReference<Channel> leaderChannel = new AtomicReference<>();

    private final Set<NetAddress> coordinatorAddresses = new HashSet<>();
    private final Map<NetAddress, Channel> listenLeaderChannels = new ConcurrentHashMap<>();

    private long lastUpdateLeaderTime;
    private long lastUpdateNotLeaderChannelsTime;


    private AtomicBoolean refresh = new AtomicBoolean(false);

    public CoordinatorConnector(List<NetAddress> coordinatorAddresses) {
        this.coordinatorAddresses.addAll(coordinatorAddresses);
        refresh();
    }

    @Override
    public Channel newChannel() {
        int times = 5;
        int sleep = 200;
        while (!verify() && times-- > 0) {
            try {
                Thread.sleep(sleep);
                refresh();
                sleep += sleep;
            } catch (InterruptedException e) {
                log.error("Wait coordinator connector ready, but interrupted.");
            }
        }
        return netService.newChannel(leaderChannel.get().remoteAddress());
    }

    @Override
    public boolean verify() {
        return leaderChannel.get() != null && leaderChannel.get().status() == Channel.Status.ACTIVE;
    }

    @Override
    public void refresh() {
        if (refresh.compareAndSet(false, true)) {
            executorService.submit(this::initChannels);
        }
    }

    private void initChannels() {
        Channel channel;
        for (NetAddress address : coordinatorAddresses) {
            try {
                channel = netService.newChannel(address);
            } catch (Exception e) {
                log.error("Open coordinator channel error, address: {}", address, e);
                continue;
            }
            channel.registerMessageListener((m, c) -> {
                c.registerMessageListener(this::connectLeader);
                c.send(GET_LEADER_LOCATION.message());
            });
            channel.send(PING.message(Tags.RAFT_SERVICE));
            return;
        }
    }

    private void connected(Message message, Channel channel) {
        log.info("Connected coordinator [{}] channel.", channel.remoteAddress());
        coordinatorAddresses.add(channel.remoteAddress());
        channel.closeListener(this::listenClose);
        channel.registerMessageListener(this::listenLeader);
        channel.send(RaftServiceCode.LISTEN_LEADER.message());
    }

    private void connectLeader(Message message, Channel channel) {
        byte[] bytes = message.toBytes();
        if (bytes == null || bytes.length == 0) {
            closeChannel(channel);
            initChannels();
            return;
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        String host = PrimitiveCodec.readString(buffer);
        Integer port = PrimitiveCodec.readVarInt(buffer);
        NetAddress leaderAddress = new NetAddress(host, port);
        try {
            Channel newLeaderChannel = netService.newChannel(leaderAddress);
            listenLeader(null, newLeaderChannel);

            newLeaderChannel.registerMessageListener((m, c) -> {
                newLeaderChannel.registerMessageListener(this::connectAll);
                c.send(GET_ALL_LOCATION.message());
            });
            newLeaderChannel.send(PING.message(Tags.RAFT_SERVICE));

            lastUpdateLeaderTime = System.currentTimeMillis();
            log.info("Connect coordinator leader success, remote: [{}]", newLeaderChannel.remoteAddress());
        } catch (Exception e) {
            log.error("Open coordinator leader channel error, address: {}", leaderAddress, e);
            refresh.set(false);
            if (!verify()) {
                refresh();
            }
        } finally {
            closeChannel(channel);
        }
    }

    private void closeChannel(Channel channel) {
        try {
            channel.close();
        } catch (Exception e) {
            log.error("Close coordinator channel error, address: [{}].", channel.remoteAddress(), e);
        }
    }

    private void connectAll(Message message, Channel channel) {
        connected(null, channel);
        byte[] buf = message.toBytes();
        if (buf.length == 0) {
            channel.send(GET_ALL_LOCATION.message());
            return;
        }
        ByteArrayInputStream bais = new ByteArrayInputStream(buf);
        int size = PreParameters.cleanNull(PrimitiveCodec.readVarInt(bais), 0);
        IntStream.range(0, size).forEach(i -> {
            String host = PrimitiveCodec.readString(bais);
            Integer port = PrimitiveCodec.readVarInt(bais);
            NetAddress address = new NetAddress(host, port);
            coordinatorAddresses.add(address);
        });
        coordinatorAddresses.stream()
            .filter(address -> !address.equals(channel.remoteAddress()))
            .forEach(address -> executorService.submit(() -> listenLeaderChannels.computeIfAbsent(address, a -> {
                try {
                    Channel ch = netService.newChannel(address);
                    ch.registerMessageListener(this::connected);
                    ch.send(PING.message(Tags.RAFT_SERVICE));
                    log.info("Open coordinator channel, address: [{}]", address);
                    return ch;
                } catch (Exception e) {
                    log.error("Open coordinator channel error, address: {}", address, e);
                }
                return null;
            })));
        lastUpdateNotLeaderChannelsTime = System.currentTimeMillis();
    }

    private void listenLeader(Message message, Channel channel) {
        Channel oldLeader = this.leaderChannel.getAndSet(channel);
        lastUpdateLeaderTime = System.currentTimeMillis();
        log.info("Coordinator leader channel changed, new leader remote: [{}], old leader remote: [{}]",
            channel.remoteAddress(),
            oldLeader == null ? null : oldLeader.remoteAddress()
        );

        if (oldLeader != null) {
            closeChannel(oldLeader);
        }
        refresh.set(false);
    }

    private void listenClose(Channel channel) {
        this.listenLeaderChannels.remove(channel.remoteAddress(), channel);
        log.info("Coordinator channel closed, remote: [{}]", channel.remoteAddress());
        if (this.leaderChannel.compareAndSet(channel, null)) {
            lastUpdateLeaderTime = System.currentTimeMillis();
            log.info("Coordinator leader channel closed, remote: [{}], then refresh", channel.remoteAddress());
        }
    }

}
