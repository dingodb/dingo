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

package io.dingodb.client.mappers;


import io.dingodb.client.IBaseDingoMapper;
import io.dingodb.client.utils.DeferredObjectLoader;
import io.dingodb.client.utils.TypeUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class MapMapper extends TypeMapper {

    @SuppressWarnings("unused")
    private final Class<?> referencedClass;
    @SuppressWarnings("unused")
    private final Class<?> itemClass;
    @SuppressWarnings("unused")
    private final Class<?> keyClass;
    private final IBaseDingoMapper mapper;
    private final boolean supportedWithoutTranslation;
    private final TypeMapper itemMapper;
    private final TypeMapper keyMapper;

    public MapMapper(final Class<?> clazz, final Class<?> keyClass, final Class<?> itemClass,
                     final TypeMapper keyMapper, final TypeMapper itemMapper, final IBaseDingoMapper mapper) {
        this.referencedClass = clazz;
        this.mapper = mapper;
        this.keyMapper = keyMapper;
        this.keyClass = keyClass;
        this.itemMapper = itemMapper;
        this.itemClass = itemClass;
        boolean isSupportTransaction = TypeUtils.isDingoNativeType(itemClass) && TypeUtils.isDingoNativeType(keyClass);
        this.supportedWithoutTranslation = isSupportTransaction;
    }

    @Override
    public Object toDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        Map<?, ?> map = (Map<?, ?>) value;
        if (map.size() == 0 || this.supportedWithoutTranslation) {
            return value;
        }

        Map<Object, Object> results = new HashMap<>();
        for (Object key : map.keySet()) {
            Object item = map.get(key);
            TypeMapper mapper1 = TypeUtils.getMapper(
                key.getClass(),
                TypeUtils.AnnotatedType.getDefaultAnnotateType(),
                mapper);
            TypeMapper keyMap = keyMapper != null ? keyMapper : mapper1;

            TypeMapper mapper2 = TypeUtils.getMapper(
                item.getClass(),
                TypeUtils.AnnotatedType.getDefaultAnnotateType(),
                mapper);
            TypeMapper itemMap = itemMapper != null ? itemMapper : mapper2;
            results.put(keyMap.toDingoFormat(key), itemMap.toDingoFormat(item));
        }
        return results;
    }

    @Override
    public Object fromDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        Map<?, ?> map = (Map<?, ?>) value;
        if (map.size() == 0 || this.supportedWithoutTranslation) {
            return value;
        }

        final Map<Object, Object> results = new TreeMap<>();
        for (Object key : map.keySet()) {
            Object item = map.get(key);

            TypeMapper mapper1 = TypeUtils.getMapper(
                key.getClass(),
                TypeUtils.AnnotatedType.getDefaultAnnotateType(),
                mapper);
            TypeMapper keyMap = keyMapper != null ? keyMapper : mapper1;

            TypeMapper mapper2 =  TypeUtils.getMapper(
                item.getClass(),
                TypeUtils.AnnotatedType.getDefaultAnnotateType(),
                mapper);

            TypeMapper itemMap = itemMapper != null ? itemMapper : mapper2;
            final Object javaKey = keyMap == null ? null : keyMap.fromDingoFormat(key);
            final Object javaItem = itemMap == null ? null : itemMap.fromDingoFormat(item);
            if (javaKey instanceof DeferredObjectLoader.DeferredObject
                || javaItem instanceof DeferredObjectLoader.DeferredObject) {
                DeferredObjectLoader.DeferredSetter setter = object -> results.put(javaKey, object);
                DeferredObjectLoader.DeferredObjectSetter objectSetter = new DeferredObjectLoader.DeferredObjectSetter(
                    setter,
                    (DeferredObjectLoader.DeferredObject) javaItem);
                DeferredObjectLoader.add(objectSetter);
            } else {
                results.put(javaKey, javaItem);
            }
        }
        return results;
    }
}
