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
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.privilege.PrivilegeDict;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.net.MessageListener;
import io.dingodb.net.NetService;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import io.dingodb.server.protocol.Tags;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class ReloadHandler {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    public static final ReloadHandler handler = new ReloadHandler();

    public void registryReloadChannel() {
        Executors.submit("coordinator-registry-flush", this::registryChannel);
    }

    public void registryChannel() {
        int times = 10;
        int sleep = 500;
        while (!CoordinatorConnector.getDefault().verify() && times-- > 0) {
            try {
                Thread.sleep(sleep);
                sleep += sleep;
            } catch (InterruptedException e) {
                log.error("Wait coordinator connector ready, but interrupted.");
            }
        }
        Channel channel = NetService.getDefault().newChannel(CoordinatorConnector.getDefault().get());
        channel.setMessageListener(reload());
        channel.send(new Message(Tags.LISTEN_REGISTRY_RELOAD, "registry reload channel".getBytes()));
    }

    public MessageListener reload() {
        return (message, ch) -> {
            if (message.tag().equals(Tags.LISTEN_RELOAD_PRIVILEGES)) {
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
