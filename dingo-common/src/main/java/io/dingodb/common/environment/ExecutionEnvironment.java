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

package io.dingodb.common.environment;

import io.dingodb.common.CommonId;
import io.dingodb.common.auth.DingoRole;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.store.KeyValue;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class ExecutionEnvironment {

    public static ExecutionEnvironment getExecutionEnvironment() {
        return INSTANCE;
    }

    public static ExecutionEnvironment INSTANCE = new ExecutionEnvironment();

    public static Map<Object, Map<String, KeyValue>> memoryCache = new HashMap<>();

    @Getter
    @Setter
    private DingoRole role;

    public Properties properties = new Properties();

    @Setter
    @Getter
    private volatile Map<String, PrivilegeGather> privilegeGatherMap = new ConcurrentHashMap<>();

    @Setter
    @Getter
    public volatile Map<String, CommonId> schemaIdMap = new ConcurrentHashMap<>();

    @Setter
    @Getter
    private volatile Map<CommonId, Map<String, CommonId>> tableIdMap = new ConcurrentHashMap<>();

    public boolean containsKey(String key) {
        return properties.containsKey(key);
    }

    public Object getInfo(String field) {
        if (properties.containsKey(field)) {
            return properties.getProperty(field);
        } else {
            return null;
        }
    }

    public String getUser() {
        if (properties.containsKey("user")) {
            return properties.getProperty("user");
        } else {
            return null;
        }
    }

    public String getPassword() {
        if (properties.containsKey("password")) {
            return properties.getProperty("password");
        } else {
            return null;
        }
    }

    public String getHost() {
        if (properties.containsKey("host")) {
            return properties.getProperty("host");
        } else {
            return null;
        }
    }

    public void remove(String field) {
        properties.remove(field);
    }

    public void setInfo(String key, Object value) {
        properties.put(key, value);
    }

    public void putAll(Properties properties) {
        this.properties.putAll(properties);
    }
}
