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

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public final class ReflectUtils {
    private static final Map<Class<?>, List<Field>> classFields = new ConcurrentHashMap<>();

    private ReflectUtils() {
    }

    public static List<String> getFields(Class<?> cls) {
        return classFields.computeIfAbsent(cls, c ->
                Arrays.stream(c.getDeclaredFields()).peek(__ -> __.setAccessible(true)).collect(Collectors.toList()))
            .stream()
            .map(Field::getName)
            .collect(Collectors.toList());
    }

    public static Map<String, String> getStrValues(Class cls, Object instance) throws IllegalAccessException {
        Map<String, String> values = new HashMap<>();
        List<Field> fields = classFields.get(cls);
        if (fields == null) {
            getFields(cls);
            fields = classFields.get(cls);
        }
        for (Field field : fields) {
            values.put(field.getName(), String.valueOf(field.get(instance)));
        }
        return values;
    }

    public static Map<String, Object> getValues(Class cls, Object instance) throws IllegalAccessException {
        Map<String, Object> values = new HashMap<>();
        List<Field> fields = classFields.get(cls);
        if (fields == null) {
            getFields(cls);
            fields = classFields.get(cls);
        }
        for (Field field : fields) {
            values.put(field.getName(), field.get(instance));
        }
        return values;
    }

}
