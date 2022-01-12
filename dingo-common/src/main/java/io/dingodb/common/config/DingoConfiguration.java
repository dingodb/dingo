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

import lombok.extern.slf4j.Slf4j;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

@Slf4j
public class DingoConfiguration {

    public static final DingoConfiguration INSTANCE = new DingoConfiguration();
    private final Map<String, Object> configs = new HashMap<>();

    private DingoConfiguration() {
    }

    public static DingoConfiguration instance() {
        return INSTANCE;
    }

    public void set(String name, Object value) {
        configs.put(name, value);
    }

    public Object get(String name) {
        Object value;
        if ((value = configs.get(name)) != null) {
            return value;
        }
        String[] names = name.split("\\.");
        Map<String, Object> configs = this.configs;
        for (String s : names) {
            value = configs.get(s);
            if (!(value instanceof Map)) {
                break;
            }
            configs = (Map<String, Object>) value;
        }
        return value;
    }

    public Boolean exist(String name) {
        return get(name) != null;
    }

    public void setBool(String name, Boolean value) {
        set(name, value);
    }

    public Boolean getBool(String name) {
        return Boolean.valueOf(getString(name));
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
            return Integer.parseInt(getString(name));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public Long getLong(String name, Long defaultValue) {
        try {
            return Long.parseLong(getString(name));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public <T> List<T> getList(String name) {
        Object o = get(name);
        if (o instanceof String) {
            o = Arrays.asList(((String) o).split(","));
        }
        return (List<T>) o;
    }

    public Properties toProperties() {
        Properties properties = new Properties();
        properties.putAll(configs);
        return properties;
    }

    public <T> T getAndConvert(String name, Class<T> cls, Supplier<T> defaultValueSupplier) {
        try {
            T result = mapToBean((Map<String, Object>) get(name), cls);
            if (result == null) {
                return defaultValueSupplier.get();
            }
            return result;
        } catch (Exception e) {
            log.error("Get configuration and convert error, name: {}, class: {}", name, cls.getName(), e);
            return defaultValueSupplier.get();
        }
    }

    public <T> T getAndConvert(String name, Class<T> cls) {
        return getAndConvert(name, cls, () -> null);
    }

    public <T> T mapToBean(Map<String, Object> map, Class<T> cls) throws Exception {
        if (map == null) {
            return null;
        }

        T obj = cls.newInstance();

        BeanInfo beanInfo = Introspector.getBeanInfo(obj.getClass());
        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
        for (PropertyDescriptor property : propertyDescriptors) {
            Object value;
            if ((value = map.get(property.getName())) == null) {
                continue;
            }
            if (value instanceof Map && !property.getPropertyType().equals(Map.class)) {
                value = mapToBean((Map<String, Object>) value, property.getPropertyType());
            }
            if (!property.getPropertyType().equals(value.getClass())) {
                value = tryConvertValue(value, property.getPropertyType());
            }
            Method setter = property.getWriteMethod();
            if (setter != null) {
                setter.invoke(obj, value);
            } else {
                Field field = cls.getDeclaredField(property.getName());
                field.setAccessible(true);
                field.set(obj, map.get(field.getName()));
            }
        }

        return obj;
    }

    private Object tryConvertValue(Object obj, Class<?> type) {
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
