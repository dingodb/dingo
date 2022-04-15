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

import io.dingodb.common.Location;
import io.dingodb.common.concurrent.ThreadPoolBuilder;
import io.dingodb.common.error.CommonError;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.NetAddress;
import io.dingodb.net.NetAddressProvider;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.net.SimpleMessage;
import io.dingodb.server.api.CoordinatorServerApi;
import io.dingodb.server.client.config.ClientConfiguration;
import io.dingodb.server.client.connector.Connector;
import io.dingodb.server.protocol.Tags;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static io.dingodb.common.util.NoBreakFunctionWrapper.wrap;

@Slf4j
public class CoordinatorConnector implements Connector, NetAddressProvider {

    private static final ExecutorService executorService = new ThreadPoolBuilder().name("CoordinatorConnector").build();
    private static final CoordinatorConnector DEFAULT_CONNECTOR;

    static {
        final String coordSrvList = ClientConfiguration.coordinatorExchangeSvrList();
        List<String> servers = Arrays.asList(coordSrvList.split(","));

        List<NetAddress> addrList = servers.stream()
            .map(s -> s.split(":"))
            .map(ss -> new NetAddress(ss[0], Integer.parseInt(ss[1])))
            .collect(Collectors.toList());

        DEFAULT_CONNECTOR = new CoordinatorConnector(addrList);
    }

    public static CoordinatorConnector defaultConnector() {
        return DEFAULT_CONNECTOR;
    }

    private final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();
    private final AtomicReference<Channel> leaderChannel = new AtomicReference<>();
    private final AtomicReference<NetAddress> leaderAddress = new AtomicReference<>();

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
        get();
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

    @Override
    public NetAddress get() {
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
        if (!verify()) {
            CommonError.EXEC_TIMEOUT.throwFormatError("wait connector available", Thread.currentThread().getName(), "");
        }
        return leaderChannel.get().remoteAddress();
    }

    private void initChannels() {
        for (NetAddress address : coordinatorAddresses) {
            try {
                Channel channel;
                CoordinatorServerApi api = netService.apiRegistry().proxy(CoordinatorServerApi.class, () -> address);
                Location leader = api.leader();
                NetAddress leaderAddress = new NetAddress(leader.getHost(), leader.getPort());
                channel = netService.newChannel(leaderAddress);
                connectedLeader(channel);
                return;
            } catch (Exception e) {
                log.error("Open coordinator channel error, address: {}", address, e);
            }
        }
        refresh.set(false);
    }

    private void connected(Message message, Channel channel) {
        log.info("Connected coordinator [{}] channel.", channel.remoteAddress());
        coordinatorAddresses.add(channel.remoteAddress());
        channel.closeListener(this::listenClose);
        channel.registerMessageListener(this::listenLeader);
        channel.send(new SimpleMessage(Tags.LISTEN_RAFT_LEADER, ByteArrayUtils.EMPTY_BYTES));
    }

    private void connectedLeader(Channel channel) {
        try {
            if (!leaderChange(channel)) {
                channel.close();
                return;
            }
            lastUpdateLeaderTime = System.currentTimeMillis();
            NetAddressProvider leaderAddress = channel::remoteAddress;
            coordinatorAddresses.addAll(netService.apiRegistry()
                .proxy(CoordinatorServerApi.class, leaderAddress).getAll().stream()
                .map(location -> new NetAddress(location.getHost(), location.getPort()))
                .collect(Collectors.toList()));
            coordinatorAddresses.stream()
                .filter(address -> !address.equals(channel.remoteAddress()))
                .forEach(address -> executorService.submit(() -> listenLeaderChannels.computeIfAbsent(
                    address,
                    wrap(this::connectFollow, e -> log.error("Open follow channel error, address: {}", address, e)))
                ));
            lastUpdateNotLeaderChannelsTime = System.currentTimeMillis();
            log.info("Connected coordinator leader success, remote: [{}]", channel.remoteAddress());
        } catch (Exception e) {
            log.error("Connected coordinator leader error, address: {}", channel, e);
        }
    }

    @Nonnull
    private Channel connectFollow(NetAddress address) {
        Channel ch = netService.newChannel(address);
        ch.registerMessageListener(this::connected);
        ch.send(new SimpleMessage(Tags.LISTEN_RAFT_LEADER, ByteArrayUtils.EMPTY_BYTES));
        log.info("Open coordinator channel, address: [{}]", address);
        return ch;
    }

    private void closeChannel(Channel channel) {
        try {
            channel.close();
        } catch (Exception e) {
            log.error("Close coordinator channel error, address: [{}].", channel.remoteAddress(), e);
        }
    }

    private void listenLeader(Message message, Channel channel) {
        leaderChange(channel);
    }

    private synchronized boolean leaderChange(Channel channel) {
        Channel oldLeader = this.leaderChannel.get();
        if (oldLeader != null && channel.remoteAddress().equals(oldLeader.remoteAddress())) {
            log.info("Coordinator leader not changed, remote: {}", channel.remoteAddress());
            return false;
        }
        if (!this.leaderChannel.compareAndSet(oldLeader, channel)) {
            if (!this.leaderChannel.compareAndSet(null, channel)) {
                return false;
            }
        }
        lastUpdateLeaderTime = System.currentTimeMillis();
        log.info("Coordinator leader channel changed, new leader remote: [{}], old leader remote: [{}]",
            channel.remoteAddress(),
            oldLeader == null ? null : oldLeader.remoteAddress()
        );

        if (oldLeader != null) {
            closeChannel(oldLeader);
        }
        refresh.set(false);
        return true;
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
