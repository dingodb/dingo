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

import io.dingodb.common.config.DingoConfiguration;
import lombok.Getter;

@Getter
public class NetServiceConfiguration {

    public static final int MIN_HEARTBEAT = 3;
    public static final int MIN_QUEUE_CAPACITY = Runtime.getRuntime().availableProcessors() * 2;
    public static final String HEARTBEAT = "heartbeat";

    public static final NetServiceConfiguration INSTANCE;

    static {
        try {
            DingoConfiguration dingoConfiguration = DingoConfiguration.instance();
            if (dingoConfiguration == null) {
                INSTANCE = new NetServiceConfiguration();
                INSTANCE.host = "127.0.0.1";
            } else {
                dingoConfiguration.setNet(NetServiceConfiguration.class);
                INSTANCE = dingoConfiguration.getNet();
                if (INSTANCE.host == null) {
                    INSTANCE.host = dingoConfiguration.getHost();
                }
            }
            if (INSTANCE.heartbeat == null || INSTANCE.heartbeat < MIN_HEARTBEAT) {
                INSTANCE.heartbeat = MIN_HEARTBEAT;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static NetServiceConfiguration instance() {
        return INSTANCE;
    }

    private Integer heartbeat;
    private String host;
    private Integer apiTimeout;

    public static Integer heartbeat() {
        return INSTANCE.heartbeat;
    }

    public static String host() {
        return INSTANCE.host;
    }

    public static Integer apiTimeout() {
        return INSTANCE.apiTimeout == null ? 30 : INSTANCE.apiTimeout;
    }
}
