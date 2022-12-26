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

package io.dingodb.server.client.reload;

import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.concurrent.ThreadPoolBuilder;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.privilege.PrivilegeDict;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.MessageListener;
import io.dingodb.net.NetService;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import io.dingodb.server.protocol.ListenerTags;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static io.dingodb.server.protocol.ListenerTags.LISTEN_REGISTRY_RELOAD;
import static io.dingodb.server.protocol.ListenerTags.LISTEN_RELOAD_PRIVILEGES;

@Slf4j
public class ReloadHandler {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    public static final ReloadHandler handler = new ReloadHandler();

    private volatile Channel channel;

    private ScheduledFuture<?> reloadFuture;

    public void registryReloadChannel() {
        Executors.submit("coordinator-registry-flush", this::registryChannel);

        reloadFuture = Executors.scheduleAtFixedRateAsync("reload", this::checkChannel,
            5, 10, TimeUnit.SECONDS);
    }

    public void checkChannel() {
        if (channel != null && channel.isClosed()) {
            registryChannel();
        }
    }

    public synchronized void registryChannel() {
        if ((channel != null && channel.isClosed()) || channel == null) {
            channel = NetService.getDefault().newChannel(CoordinatorConnector.getDefault().get());
            channel.setMessageListener(reload());
            channel.send(new Message(LISTEN_REGISTRY_RELOAD, "registry reload channel".getBytes()));
            log.info("registryChannel success:" + channel.remoteLocation().toString());
        }
    }

    public MessageListener reload() {
        return (message, ch) -> {
            if (message.tag().equals(LISTEN_RELOAD_PRIVILEGES)) {
                PrivilegeGather privilegeGather = ProtostuffCodec.read(message.content());
                if (privilegeGather.getHost().equals("%")) {
                    env.getPrivilegeGatherMap().forEach((k, v) -> {
                        if (k.startsWith(privilegeGather.getUser() + "#")) {
                            env.getPrivilegeGatherMap().put(k, privilegeGather);
                        }
                    });
                }
                env.getPrivilegeGatherMap().put(privilegeGather.key(), privilegeGather);
                log.info("reload privileges:" + env.getPrivilegeGatherMap());
            }
        };
    }
}
