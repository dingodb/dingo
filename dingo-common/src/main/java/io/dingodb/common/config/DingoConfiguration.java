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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DingoConfiguration {

    public static final DingoConfiguration INSTANCE = new DingoConfiguration();
    private final Map<String, Object> configs = new HashMap<>();

    private DingoConfiguration() {
    }

    public static DingoConfiguration instance() {
        return INSTANCE;
    }

    public void set(String name, Object value) {
        configs.put(name, value.toString());
    }

    public Object get(String name) {
        return configs.get(name);
    }

    public void setBool(String name, Boolean value) {
        set(name, value);
    }

    public Boolean getBool(String name) {
        return Boolean.valueOf(getString(name, null));
    }

    public void setString(String name, String value) {
        set(name, value);
    }

    public String getString(String name) {
        return getString(name, null);
    }

    public String getString(String name, String defaultValue) {
        try {
            return get(name).toString();
        } catch (NullPointerException e) {
            return defaultValue;
        }
    }

    public void setInt(String name, int value) {
        set(name, value);
    }

    public Integer getInt(String name) {
        return getInt(name, null);
    }

    public Integer getInt(String name, Integer defaultValue) {
        try {
            return Integer.parseInt(configs.get(name).toString());
        } catch (NullPointerException e) {
            return defaultValue;
        }
    }

    public Properties toProperties() {
        Properties properties = new Properties();
        properties.putAll(configs);
        return properties;
    }

}
