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

package io.dingodb.common.util;

import lombok.SneakyThrows;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public final class ReflectionUtils {
    private static final Map<Class<?>, List<Field>> classFields = new ConcurrentHashMap<>();

    private ReflectionUtils() {
    }

    @SneakyThrows
    public static <T> T convert(Map<String, Object> src, Class<T> targetType) {
        if (src == null) {
            return null;
        }

        T obj = newInstance(targetType);

        Field[] fields = targetType.getDeclaredFields();
        for (Field field : fields) {
            try {
                Object value;
                if ((value = src.get(field.getName())) == null) {
                    continue;
                }
                if (value instanceof Map && !field.getType().equals(Map.class)) {
                    value = convert((Map<String, Object>) value, field.getType());
                }
                if (!field.getType().equals(value.getClass())) {
                    value = tryConvertValue(value, field.getType());
                }
                field.setAccessible(true);
                field.set(obj, value);
            } catch (Exception e) {
                throw new RuntimeException(
                    "Parse " + targetType.getName() + "." + field.getName() + " error, " + e.getMessage(), e
                );
            }
        }
        return obj;
    }

    @SneakyThrows
    public static <T> T newInstance(@NonNull Class<T> cls) {
        Constructor<T> constructor = cls.getDeclaredConstructor();
        constructor.setAccessible(true);
        return constructor.newInstance();
    }

    private static Object tryConvertValue(@NonNull Object obj, @NonNull Class<?> type) {
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
