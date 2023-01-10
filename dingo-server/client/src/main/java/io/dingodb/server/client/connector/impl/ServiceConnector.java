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
import io.dingodb.common.concurrent.LinkedRunner;
import io.dingodb.common.error.CommonError;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.NetService;
import io.dingodb.net.service.ListenService;
import io.dingodb.server.client.connector.Connector;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.dingodb.common.util.DebugLog.debug;

@Slf4j
public class ServiceConnector implements Connector, Supplier<Location> {

    public static final  String MPU_PRIMARY = "MPU-PRIMARY";

    private final LinkedRunner runner;
    private final CommonId serviceId;
    private final AtomicReference<Location> leader = new AtomicReference<>();

    private Set<Location> addresses = new CopyOnWriteArraySet<>();
    private final Map<Location, ListenService.Future> listenedAddresses = new ConcurrentHashMap<>();

    public ServiceConnector(CommonId serviceId, Set<Location> addresses) {
        this.serviceId = serviceId;
        this.addresses.addAll(addresses);
        this.runner = new LinkedRunner("service-connector-" + serviceId);
        refresh();
    }

    public Set<Location> getAddresses() {
        return new HashSet<>(addresses);
    }

    @Override
    public void close() {
        listenedAddresses.values().forEach(ListenService.Future::cancel);
        listenedAddresses.clear();
    }

    @Override
    public Channel newChannel() {
        return NetService.getDefault().newChannel(get());
    }

    @Override
    public boolean verify() {
        return !leader.compareAndSet(null, null);
    }

    @Override
    public void refresh() {
        addresses.forEach(location -> runner.forceFollow(() -> this.listen(location)));
    }

    @Override
    public Location get() {
        int times = 10;
        int sleep = 1000;
        while (!verify() && times-- > 0) {
            try {
                Thread.sleep(sleep);
                runner.forceFollow(this::refresh);
            } catch (InterruptedException e) {
                log.error("Wait service [{}] connector ready, but interrupted.", serviceId);
            }
        }
        if (!verify()) {
            CommonError.EXEC_TIMEOUT.throwFormatError("wait connector available", Thread.currentThread().getName(), "");
        }
        return leader.get();
    }

    private void listen(Location location) {
        if (listenedAddresses.containsKey(location)) {
            listenedAddresses.get(location).cancel();
        }
        ListenService.Future future = ListenService.getDefault().listen(
            serviceId,
            MPU_PRIMARY,
            location,
            msg -> runner.forceFollow(() -> onCallback(msg, location)),
            () -> runner.forceFollow(() -> onClose(location))
        );
        listenedAddresses.put(location, future);
    }

    private void onClose(Location location) {
        listenedAddresses.remove(location);
        debug(log, "Service [{}] channel closed, remote: [{}]", serviceId, location);
        if (leader.compareAndSet(location, null)) {
            debug(log, "Service [{}] leader channel closed, remote: [{}].", serviceId, location);
        }
    }

    private void onCallback(Message message, Location location) {
        Location leader = this.leader.get();
        if (location.equals(leader)) {
            // todo on change service distribute
            //this.addresses = ProtostuffCodec.read(message.content());
            debug(log, "Service [{}] update locations to : {}", serviceId, addresses);
        } else {
            this.leader.set(location);
            debug(log,
                "Service [{}] leader channel changed, new leader remote: [{}], old leader remote: [{}]",
                serviceId, location, leader
            );
        }
    }

}
