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

package io.dingodb.server.executor.config;

import io.dingodb.common.config.DingoConfiguration;
import lombok.experimental.Delegate;

@Deprecated
public class ExecutorConfiguration {

    public static final ExecutorConfiguration INSTANCE = new ExecutorConfiguration();

    public static final String EXECUTOR_SERVER_PORT = "executor.server.port";
    private static final String STORE_KEY = "active.store";

    @Delegate
    private final DingoConfiguration dingoConfiguration = DingoConfiguration.instance();

    private ExecutorConfiguration() {
    }

    public static ExecutorConfiguration instance() {
        return INSTANCE;
    }

    public ExecutorConfiguration store(String store) {
        setString(STORE_KEY, store);
        return this;
    }

    public String store() {
        return getString(STORE_KEY);
    }

    /*
    public int port() {
        return getInt(EXECUTOR_SERVER_PORT, instancePort());
    }*/

}
