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

package io.dingodb.common.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.annotation.Nonnull;

import static io.dingodb.expr.json.runtime.Parser.YAML;

@Getter
@Setter
@ToString
@Slf4j
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class DingoConfiguration {

    private static DingoConfiguration INSTANCE;

    public static synchronized void parse(final String configPath) throws Exception {
        INSTANCE = YAML.parse(new FileInputStream(configPath), DingoConfiguration.class);
    }

    public static DingoConfiguration instance() {
        return INSTANCE;
    }

    private ClusterConfiguration cluster;
    private CommonId serverId;

    @Delegate
    private ExchangeConfiguration exchange;

    private Map<String, Object> server;
    private Map<String, Object> store;
    private Map<String, Object> net;
    private Map<String, Object> client;

    @JsonIgnore
    private Object serverConfiguration;
    @JsonIgnore
    private Object storeConfiguration;
    @JsonIgnore
    private Object netConfiguration;
    @JsonIgnore
    private Object clientConfiguration;

    public static String host() {
        return INSTANCE == null ? null : INSTANCE.getHost();
    }

    public static int port() {
        return INSTANCE == null ? 0 : INSTANCE.getPort();
    }

    public static int raftPort() {
        return INSTANCE == null ? 0 : INSTANCE.getRaftPort();
    }

    public static CommonId serverId() {
        return INSTANCE.serverId;
    }

    public static Location location() {
        return new Location(host(), port(), raftPort());
    }

    public void setServer(Class<?> cls) throws Exception {
        serverConfiguration = mapToBean(server, cls);
        if (serverConfiguration == null) {
            serverConfiguration = newInstance(cls);
        }
    }

    public void setStore(Class<?> cls) throws Exception {
        storeConfiguration = mapToBean(store, cls);
        if (storeConfiguration == null) {
            storeConfiguration = newInstance(cls);
        }
    }

    public void setNet(Class<?> cls) throws Exception {
        netConfiguration = mapToBean(net, cls);
        if (netConfiguration == null) {
            netConfiguration = newInstance(cls);
        }
    }

    public void setClient(Class<?> cls) throws Exception {
        clientConfiguration = mapToBean(client, cls);
        if (clientConfiguration == null) {
            clientConfiguration = newInstance(cls);
        }
    }

    public <T> T getServer() {
        return (T) serverConfiguration;
    }

    public <T> T getNet() {
        return (T) netConfiguration;
    }

    public <T> T getStore() {
        return (T) storeConfiguration;
    }

    public int getRaftPort() {
        Object raftPort = ((Map<String, Object>) store.get("raft")).get("port");
        if (raftPort == null) {
            log.error("Miss configuration store->raft->port");
            return 0;
        }
        if (raftPort instanceof Integer) {
            return (Integer) raftPort;
        } else if (raftPort instanceof String) {
            return Integer.parseInt((String) raftPort);
        }
        log.error("Cannot cast raft port");
        return 0;
    }

    public <T> T getClient() {
        return (T) clientConfiguration;
    }

    public static <T> T mapToBean(Map<String, Object> map, Class<T> cls) throws Exception {
        if (map == null) {
            return null;
        }

        T obj = newInstance(cls);

        Field[] fields = cls.getDeclaredFields();
        for (Field field : fields) {
            try {
                Object value;
                if ((value = map.get(field.getName())) == null) {
                    continue;
                }
                if (value instanceof Map && !field.getType().equals(Map.class)) {
                    value = mapToBean((Map<String, Object>) value, field.getType());
                }
                if (!field.getType().equals(value.getClass())) {
                    value = tryConvertValue(value, field.getType());
                }
                field.setAccessible(true);
                field.set(obj, value);
            } catch (Exception e) {
                log.error("parse property name: {}. class name: {};", field.getName(), cls.getName(), e);
                throw e;
            }
        }
        return obj;
    }

    @Nonnull
    private static <T> T newInstance(Class<T> cls)
        throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        Constructor<T> constructor = cls.getDeclaredConstructor();
        constructor.setAccessible(true);
        T obj = constructor.newInstance();
        return obj;
    }

    private static Object tryConvertValue(Object obj, Class<?> type) {
        String str = obj.toString();
        if (type.equals(String.class)) {
            return str;
        }
        if (type.equals(Integer.class)) {
            return Integer.parseInt(str);
        }
        if (type.equals(Double.class)) {
            return Double.parseDouble(str);
        }
        if (type.equals(Float.class)) {
            return Float.parseFloat(str);
        }
        if (type.equals(Long.class)) {
            return Long.parseLong(str);
        }
        if (type.equals(Boolean.class)) {
            if (str.matches("[0-1]")) {
                return Integer.parseInt(str) != 0;
            }
            if ("true".equalsIgnoreCase(str)) {
                return true;
            }
            if ("false".equalsIgnoreCase(str)) {
                return false;
            }
        }
        if (type.equals(byte[].class)) {
            return str.getBytes(StandardCharsets.UTF_8);
        }
        return obj;
    }

}
