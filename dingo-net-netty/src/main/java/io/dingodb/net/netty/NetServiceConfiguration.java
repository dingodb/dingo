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
import lombok.experimental.Delegate;

public class NetServiceConfiguration {

    public static final NetServiceConfiguration INSTANCE = new NetServiceConfiguration();
    public static final int DEFAULT_HEARTBEAT = 10;
    public static final int MIN_HEARTBEAT = 2;
    public static final String HEARTBEAT = "heartbeat";
    @Delegate
    private final DingoConfiguration dingoConfiguration = DingoConfiguration.INSTANCE;

    private NetServiceConfiguration() {
    }

    public static NetServiceConfiguration instance() {
        return INSTANCE;
    }

    public Integer heartbeat() {
        Integer heartbeat = getInt(HEARTBEAT, DEFAULT_HEARTBEAT);
        if (heartbeat <= MIN_HEARTBEAT) {
            return DEFAULT_HEARTBEAT;
        }
        return heartbeat;
    }

}
