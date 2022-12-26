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

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.error.CommonError;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.NetService;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.ServiceConnectApi;
import io.dingodb.server.client.connector.Connector;
import io.dingodb.server.protocol.ListenerTags;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public class ServiceConnector implements Connector, Supplier<Location> {

    private static final NetService netService = NetService.getDefault();
    private static final ApiRegistry apiRegistry = ApiRegistry.getDefault();

    private final CommonId serviceId;
    private final AtomicReference<Channel> leaderChannel = new AtomicReference<>();

    @Getter
    private final Set<Location> addresses = new HashSet<>();
    private final Map<Location, Channel> listenLeaderChannels = new ConcurrentHashMap<>();

    public ServiceConnector(CommonId serviceId, Set<Location> addresses) {
        this.serviceId = serviceId;
        this.addresses.addAll(addresses);
        refresh();
    }

    @Override
    public Channel newChannel() {
        return netService.newChannel(get());
    }

    @Override
    public boolean verify() {
        return leaderChannel.get() != null && leaderChannel.get().status() == Channel.Status.ACTIVE;
    }

    @Override
    public void refresh() {
        Executors.submit("service-connector-refresh", this::initChannels);
    }

    @Override
    public Location get() {
        int times = 10;
        int sleep = 500;
        while (!verify() && times-- > 0) {
            try {
                Thread.sleep(sleep);
                refresh();
                sleep += sleep;
            } catch (InterruptedException e) {
                log.error("Wait service connector ready, but interrupted.");
            }
        }
        if (!verify()) {
            CommonError.EXEC_TIMEOUT.throwFormatError("wait connector available", Thread.currentThread().getName(), "");
        }
        return leaderChannel.get().remoteLocation();
    }

    private void initChannels() {
        for (Location address : addresses) {
            if (verify()) {
                return;
            }
            try {
                Channel channel;
                Location leaderAddress = apiRegistry.proxy(ServiceConnectApi.class, () -> address).leader(serviceId);
                channel = netService.newChannel(leaderAddress);
                connectedLeader(channel);
                return;
            } catch (Exception e) {
                log.error("Open service channel error, address: {}", address, e);
            }
        }
    }

    private void connected(Message message, Channel channel) {
        log.info("Connected service [{}] channel.", channel.remoteLocation());
        addresses.add(channel.remoteLocation());
        channel.setCloseListener(this::listenClose);
        channel.setMessageListener(this::listenLeader);
    }

    private void connectedLeader(Channel channel) {
        try {
            if (!leaderChange(channel)) {
                channel.close();
                return;
            }
            addresses.addAll(netService.apiRegistry()
                .proxy(ServiceConnectApi.class, channel::remoteLocation).getAll(serviceId).stream()
                .map(location -> new Location(location.getHost(), location.getPort()))
                .collect(Collectors.toList()));
            addresses.stream()
                .filter(address -> !address.equals(channel.remoteLocation()))
                .forEach(address -> Executors.submit("ServiceConnector", () -> listenLeaderChannels.computeIfAbsent(
                    address,
                    wrap(this::connectFollow, e -> log.error("Open follow channel error, address: {}", address, e)))
                ));
            log.info("Connected service leader success, remote: [{}]", channel.remoteLocation());
        } catch (Exception e) {
            log.error("Connected service leader error, address: {}", channel, e);
        }
    }

    private @NonNull Channel connectFollow(Location address) {
        Channel ch = netService.newChannel(address);
        ch.setMessageListener(this::connected);
        ch.send(new Message(ListenerTags.LISTEN_SERVICE_LEADER, ByteArrayUtils.EMPTY_BYTES));
        log.info("Open service channel, address: [{}]", address);
        return ch;
    }

    private void closeChannel(Channel channel) {
        try {
            channel.close();
        } catch (Exception e) {
            log.error("Close service channel error, address: [{}].", channel.remoteLocation(), e);
        }
    }

    private void listenLeader(Message message, Channel channel) {
        log.info("Receive leader message from [{}]", channel.remoteLocation().url());
        leaderChange(channel);
    }

    private synchronized boolean leaderChange(Channel channel) {
        Channel oldLeader = this.leaderChannel.get();
        if (oldLeader != null && oldLeader.isActive() && channel.remoteLocation().equals(oldLeader.remoteLocation())) {
            log.info("Service leader not changed, remote: {}", channel.remoteLocation());
            return false;
        }
        if (!this.leaderChannel.compareAndSet(oldLeader, channel)) {
            if (!this.leaderChannel.compareAndSet(null, channel)) {
                return false;
            }
        }
        log.info("Service leader channel changed, new leader remote: [{}], old leader remote: [{}]",
            channel.remoteLocation(),
            oldLeader == null ? null : oldLeader.remoteLocation()
        );

        if (oldLeader != null) {
            closeChannel(oldLeader);
        }
        channel.setCloseListener(this::listenClose);
        return true;
    }

    private void listenClose(Channel channel) {
        this.listenLeaderChannels.remove(channel.remoteLocation());
        log.info("Service channel closed, remote: [{}]", channel.remoteLocation());
        if (this.leaderChannel.compareAndSet(channel, null)) {
            log.info("Service leader channel closed, remote: [{}].", channel.remoteLocation());
        }
    }

}
