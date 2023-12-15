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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.util.Optional;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.FileInputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.dingodb.common.util.ReflectionUtils.convert;

@Getter
@Setter
@ToString
@Slf4j
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class DingoConfiguration {

    private static final ObjectMapper PARSER = YAMLMapper.builder().build();
    private static final DingoConfiguration INSTANCE = new DingoConfiguration();

    private final Map<String, Object> config = new ConcurrentHashMap<>();

    private CommonId serverId;
    @Delegate
    private ExchangeConfiguration exchange = new ExchangeConfiguration();
    private SecurityConfiguration security = new SecurityConfiguration();
    private VariableConfiguration variable = new VariableConfiguration();

    public static synchronized void parse(final String configPath) throws Exception {
        if (configPath != null) {
            INSTANCE.copyConfig(PARSER.readValue(new FileInputStream(configPath), Map.class), INSTANCE.config);
        }
        INSTANCE.exchange = INSTANCE.getConfig("exchange", ExchangeConfiguration.class);
        INSTANCE.security = INSTANCE.getConfig("security", SecurityConfiguration.class);
        INSTANCE.variable = INSTANCE.getConfig("variable", VariableConfiguration.class);
    }

    private static void copyConfig(Map<String, Object> from, Map<String, Object> to) {
        for (Map.Entry<String, Object> entry : from.entrySet()) {
            if (entry.getValue() instanceof Map) {
                copyConfig(
                    (Map<String, Object>) entry.getValue(),
                    (Map<String, Object>) to.computeIfAbsent(entry.getKey(), k -> new ConcurrentHashMap<>())
                );
            }
            to.put(entry.getKey(), entry.getValue());
        }
    }

    public static @NonNull DingoConfiguration instance() {
        return INSTANCE;
    }

    public static String host() {
        return Optional.mapOrNull(INSTANCE.exchange, ExchangeConfiguration::getHost);
    }

    public static int port() {
        return Optional.mapOrGet(INSTANCE.exchange, ExchangeConfiguration::getPort, () -> 0);
    }

    public static CommonId serverId() {
        return INSTANCE.serverId;
    }

    public static @NonNull Location location() {
        return new Location(host(), port());
    }

    public <T> T find(String key, Class<T> targetType) {
        return find(key, targetType, config);
    }

    private <T> T find(String key, Class<T> targetType, Map<String, Object> config) {
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            if (entry.getKey().equals(key)) {
                if (targetType.isInstance(entry.getValue())) {
                    return (T) entry.getValue();
                }
            }
            if (entry.getValue() instanceof Map) {
                T target = find(key, targetType, (Map<String, Object>) entry.getValue());
                if (target != null) {
                    return target;
                }
            }
        }
        return null;
    }

    public <T> T getConfig(String key, Class<T> configType) {
        return convert(getConfigMap(key), configType);
    }

    public Map<String, Object> getConfigMap(String key) {
        return (Map<String, Object>) INSTANCE.config.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
    }

}
