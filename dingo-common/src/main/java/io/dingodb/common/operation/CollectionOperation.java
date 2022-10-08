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

package io.dingodb.common.operation;

import io.dingodb.common.operation.compute.CollectionType;
import io.dingodb.common.operation.context.ListContext;
import io.dingodb.common.operation.context.MapContext;
import io.dingodb.common.operation.context.OperationContext;
import io.dingodb.common.operation.context.UniqueListContext;
import io.dingodb.common.operation.executive.Executive;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.converter.ClientConverter;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public interface CollectionOperation<D extends OperationContext, T, R> extends Executive<D, T, R> {

    default String arrayToString(Object[] keys) {
        return String.join(",", Arrays.stream(keys).map(Object::toString).toArray(String[]::new));
    }

    default Object convertValueTo(Object value) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(value);
            byte[] result = bos.toByteArray();
            oos.close();
            bos.close();
            return ClientConverter.INSTANCE.convert(result);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    default Map<?, ?> convertValueFrom(Object value) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(ClientConverter.INSTANCE.convertBinaryFrom(value));
            ObjectInputStream ois = new ObjectInputStream(bis);
            Map<?, ?> result = (Map<?, ?>) ois.readObject();
            ois.close();
            bis.close();
            return result;
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Slf4j
    class Size implements CollectionOperation<OperationContext, Iterator<KeyValue>, Object> {

        @Override
        public DingoExecResult execute(OperationContext context, Iterator<KeyValue> records) {
            Map<String, Value> result = new HashMap<>();
            Column[] columns = context.columns;
            int[] indexes = new int[columns.length];
            for (int i = 0; i < columns.length; i++) {
                indexes[i] = context.definition.getColumnIndexOfValue(columns[i].name);
            }
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    Object[] objects = context.dingoValueCodec().decode(keyValue.getValue(), indexes);
                    Map<String, Object> values = getCollectionSize(columns, objects);
                    Object[] key = context.keyValueCodec().decodeKey(keyValue.getKey());
                    result.put(arrayToString(key), Value.get(values));
                } catch (IOException e) {
                    return new DingoExecResult(false, "Size operation decode failed, " + e.getMessage());
                }
            }
            return new DingoExecResult(result, true, "OK", CollectionType.SIZE.name());
        }

        private Map<String, Object> getCollectionSize(Column[] columns, Object[] objects) {
            Map<String, Object> values = new HashMap<>();
            for (int i = 0; i < objects.length; i++) {
                int size;
                if (objects[i] instanceof List) {
                    List list = (List) objects[i];
                    size = list.size();
                } else if (objects[i] instanceof byte[]) {
                    Map<?, ?> map = convertValueFrom(objects[i]);
                    size = map.size();
                } else {
                    size = 0;
                }
                values.put(columns[i].name, size);
            }
            return values;
        }
    }

    class GetByIndex implements CollectionOperation<ListContext, Iterator<KeyValue>, Object> {

        @Override
        public DingoExecResult execute(ListContext context, Iterator<KeyValue> records) {
            Map<String, Value> result = new HashMap<>();
            Column[] columns = context.columns;
            int[] indexes = new int[columns.length];
            for (int i = 0; i < columns.length; i++) {
                indexes[i] = context.definition.getColumnIndexOfValue(columns[i].name);
            }
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    Object[] objects = context.dingoValueCodec().decode(keyValue.getValue(), indexes);
                    Map<String, Object> values = new HashMap<>();
                    for (int i = 0; i < objects.length; i++) {
                        List value = (List) objects[i];
                        if (value.size() > context.index) {
                            values.put(columns[i].name, value.get(context.index));
                        }
                    }
                    Object[] key = context.keyValueCodec().decodeKey(keyValue.getKey());
                    result.put(arrayToString(key), Value.get(values));
                } catch (IOException e) {
                    return new DingoExecResult(false, "Get by index operation decode failed, " + e.getMessage());
                }
            }
            return new DingoExecResult(result, true, "OK", CollectionType.GET_BY_INDEX.name());
        }
    }

    class GetByIndexRange implements CollectionOperation<ListContext, Iterator<KeyValue>, Object> {

        @Override
        public DingoExecResult execute(ListContext context, Iterator<KeyValue> records) {
            Map<String, Value> result = new HashMap<>();
            Column[] columns = context.columns;
            int[] indexes = new int[columns.length];
            for (int i = 0; i < columns.length; i++) {
                indexes[i] = context.definition.getColumnIndexOfValue(columns[i].name);
            }
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    Object[] objects = context.dingoValueCodec().decode(keyValue.getValue(), indexes);
                    Map<String, Object> values = new HashMap<>();
                    for (int i = 0; i < objects.length; i++) {
                        List list = (List) objects[i];
                        int fromIndex = context.index;
                        int toIndex = Math.min(fromIndex + context.count, list.size());
                        List<Object> value;
                        if (fromIndex < 0 || fromIndex > toIndex) {
                            value = new ArrayList<>();
                        } else {
                            value = new ArrayList<Object>(list.subList(fromIndex, toIndex));
                        }
                        values.put(columns[i].name, value);
                    }
                    Object[] key = context.keyValueCodec().decodeKey(keyValue.getKey());
                    result.put(arrayToString(key), Value.get(values));
                } catch (IOException e) {
                    return new DingoExecResult(
                        false, "Get by index range operation decode failed, " + e.getMessage());
                }
            }
            return new DingoExecResult(result, true, "OK", CollectionType.GET_BY_INDEX_RANGE.name());
        }
    }

    class GetAll implements CollectionOperation<OperationContext, Iterator<KeyValue>, Object> {

        @Override
        public DingoExecResult execute(OperationContext context, Iterator<KeyValue> records) {
            Map<String, Value> result = new HashMap<>();
            Column[] columns = context.columns;
            int[] indexes = new int[columns.length];
            for (int i = 0; i < columns.length; i++) {
                indexes[i] = context.definition.getColumnIndexOfValue(columns[i].name);
            }
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    Object[] objects = context.dingoValueCodec().decode(keyValue.getValue(), indexes);
                    Map<String, Object> values = new HashMap<>();
                    for (int i = 0; i < objects.length; i++) {
                        values.put(columns[i].name, convert(objects[i]));
                    }
                    Object[] key = context.keyValueCodec().decodeKey(keyValue.getKey());

                    result.put(arrayToString(key), Value.get(values));
                } catch (IOException e) {
                    return new DingoExecResult(false, "Get all operation decode failed, " + e.getMessage());
                }
            }
            return new DingoExecResult(result, true, "OK", CollectionType.GET_ALL.name());
        }

        private Object convert(Object object) {
            if (object instanceof byte[]) {
                return convertValueFrom(object);
            }
            return object;
        }
    }

    class Set implements CollectionOperation<ListContext, Iterator<KeyValue>, Object> {

        @Override
        public List<KeyValue> execute(ListContext context, Iterator<KeyValue> records) {
            List<KeyValue> list = new ArrayList<>();
            Column[] columns = context.columns;
            int[] indexes = new int[columns.length];
            for (int i = 0; i < columns.length; i++) {
                indexes[i] = context.definition.getColumnIndexOfValue(columns[i].name);
            }
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    Object[] objects = context.dingoValueCodec().decode(keyValue.getValue(), indexes);
                    for (Object object : objects) {
                        List value = (List) object;
                        value.add(context.value.getObject());
                    }
                    byte[] bytes = context.dingoValueCodec().encode(keyValue.getValue(), objects, indexes);
                    keyValue.setValue(bytes);
                    list.add(keyValue);
                } catch (IOException e) {
                    throw new RuntimeException();
                }
            }
            return list;
        }
    }

    @Slf4j
    class Clear implements CollectionOperation<OperationContext, Iterator<KeyValue>, Object> {

        @Override
        public List<KeyValue> execute(OperationContext context, Iterator<KeyValue> records) {
            List<KeyValue> list = new ArrayList<>();
            try {
                Column[] columns = context.columns;
                int[] indexes = new int[columns.length];
                for (int i = 0; i < columns.length; i++) {
                    indexes[i] = context.definition.getColumnIndexOfValue(columns[i].name);
                }
                while (records.hasNext()) {
                    KeyValue keyValue = records.next();
                    try {
                        Object[] objects = context.dingoValueCodec().decode(keyValue.getValue(), indexes);
                        Object[] convertObj = convert(objects);
                        byte[] bytes =
                            context.dingoValueCodec().encode(keyValue.getValue(), convertObj, indexes);
                        keyValue.setValue(bytes);
                        list.add(keyValue);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            } catch (Exception e) {
                log.error("Failed to clear collection", e);
            }
            return list;
        }

        private Object[] convert(Object[] objects) {
            for (int i = 0; i < objects.length; i++) {
                Object object = objects[i];
                if (object instanceof List) {
                    List<?> list = (List<?>) object;
                    list.clear();
                    objects[i] = list;
                } else if (object instanceof byte[]) {
                    Map<?, ?> map = convertValueFrom(object);
                    map.clear();
                    objects[i] = convertValueTo(map);
                }
            }
            return objects;
        }
    }

    @Slf4j
    class RemoveByIndex implements CollectionOperation<ListContext, Iterator<KeyValue>, Object> {

        @Override
        public List<KeyValue> execute(ListContext context, Iterator<KeyValue> records) {
            List<KeyValue> list = new ArrayList<>();
            try {
                Column[] columns = context.columns;
                int[] indexes = new int[columns.length];
                for (int i = 0; i < columns.length; i++) {
                    indexes[i] = context.definition.getColumnIndexOfValue(columns[i].name);
                }
                while (records.hasNext()) {
                    KeyValue keyValue = records.next();
                    try {
                        Object[] objects = context.dingoValueCodec().decode(keyValue.getValue(), indexes);
                        for (Object object : objects) {
                            List<?> value = (List<?>) object;
                            if (value.size() > context.index) {
                                value.remove(context.index);
                            }
                        }
                        byte[] bytes =
                            context.dingoValueCodec().encode(keyValue.getValue(), objects, indexes);
                        keyValue.setValue(bytes);
                        list.add(keyValue);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            } catch (Exception e) {
                log.error("Failed to remove element based on index", e);
            }
            return list;
        }
    }

    // map
    @Slf4j
    class Put implements CollectionOperation<MapContext, Iterator<KeyValue>, Object> {

        @Override
        public List<KeyValue> execute(MapContext context, Iterator<KeyValue> records) {
            List<KeyValue> list = new ArrayList<>();
            try {
                Column[] columns = context.columns;
                int[] indexes = new int[columns.length];
                for (int i = 0; i < columns.length; i++) {
                    indexes[i] = context.definition.getColumnIndexOfValue(columns[i].name);
                }
                while (records.hasNext()) {
                    KeyValue keyValue = records.next();
                    try {
                        Object[] objects = context.dingoValueCodec().decode(keyValue.getValue(), indexes);
                        for (int i = 0; i < objects.length; i++) {
                            Map value = convertValueFrom(objects[i]);
                            value.put(context.key.getObject(), context.value.getObject());
                            objects[i] = convertValueTo(value);
                        }
                        byte[] bytes =
                            context.dingoValueCodec().encode(keyValue.getValue(), objects, indexes);
                        keyValue.setValue(bytes);
                        list.add(keyValue);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            } catch (Exception e) {
                log.error("Failed to add element to collection", e);
            }
            return list;
        }
    }

    @Slf4j
    class RemoveByKey implements CollectionOperation<MapContext, Iterator<KeyValue>, Object> {

        @Override
        public List<KeyValue> execute(MapContext context, Iterator<KeyValue> records) {
            List<KeyValue> list = new ArrayList<>();
            try {
                Column[] columns = context.columns;
                int[] indexes = new int[columns.length];
                for (int i = 0; i < columns.length; i++) {
                    indexes[i] = context.definition.getColumnIndexOfValue(columns[i].name);
                }
                while (records.hasNext()) {
                    KeyValue keyValue = records.next();
                    try {
                        Object[] objects = context.dingoValueCodec().decode(keyValue.getValue(), indexes);
                        for (int i = 0; i < objects.length; i++) {
                            Map value = convertValueFrom(objects[i]);
                            value.remove(context.key.getObject());
                            objects[i] = convertValueTo(value);
                        }

                        byte[] bytes = context.dingoValueCodec().encode(keyValue.getValue(), objects, indexes);
                        keyValue.setValue(bytes);
                        list.add(keyValue);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            } catch (Exception e) {
                log.error("Failed to delete element based on key", e);
            }
            return list;
        }
    }

    @Slf4j
    class GetByKey implements CollectionOperation<MapContext, Iterator<KeyValue>, Object> {

        @Override
        public DingoExecResult execute(MapContext context, Iterator<KeyValue> records) {
            // {key1={col1=10, col2=20}, key2={col1=5, col2=25}}
            Map<String, Value> result = new HashMap<>();
            Column[] columns = context.columns;
            int[] indexes = new int[columns.length];
            for (int i = 0; i < columns.length; i++) {
                indexes[i] = context.definition.getColumnIndexOfValue(columns[i].name);
            }

            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    Object[] objects = context.dingoValueCodec().decode(keyValue.getValue(), indexes);
                    Map<String, Object> values = new HashMap<>();
                    for (int i = 0; i < objects.length; i++) {
                        Map value = convertValueFrom(objects[i]);
                        if (value.containsKey(context.key.getObject())) {
                            values.put(columns[i].name, value.get(context.key.getObject()));
                        }
                    }
                    if (!values.isEmpty()) {
                        Object[] key = context.keyValueCodec().decodeKey(keyValue.getKey());
                        result.put(arrayToString(key), Value.get(values));
                    } else {
                        log.info("Collection get by key operation, The key:{} was not found in the map",
                            context.key.getObject());
                    }

                } catch (IOException e) {
                    return new DingoExecResult(false, "Get by key operation decode failed, " + e.getMessage());
                }
            }
            return new DingoExecResult(result, true, "OK", CollectionType.GET_BY_KEY.name());
        }
    }

    // unique list
    class GetByValue implements CollectionOperation<UniqueListContext, Iterator<KeyValue>, Object> {

        @Override
        public DingoExecResult execute(UniqueListContext context, Iterator<KeyValue> records) {
            Map<String, Value> result = new HashMap<>();
            Column[] columns = context.columns;
            int[] indexes = new int[columns.length];
            for (int i = 0; i < columns.length; i++) {
                indexes[i] = context.definition.getColumnIndexOfValue(columns[i].name);
            }
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    Map<String, Object> values = new HashMap<>();
                    Object[] objects = context.dingoValueCodec().decode(keyValue.getValue(), indexes);
                    for (int i = 0; i < objects.length; i++) {
                        List<?> value = ((List<?>) objects[i]).stream()
                            .filter(l -> l.equals(context.value.getObject()))
                            .collect(Collectors.toList());
                        values.put(columns[i].name, value);
                    }
                    Object[] key = context.keyValueCodec().decodeKey(keyValue.getKey());
                    result.put(arrayToString(key), Value.get(values));
                } catch (IOException e) {
                    return new DingoExecResult(false, "Get by value operation decode failed, " + e.getMessage());
                }
            }
            return new DingoExecResult(result, true, "OK", CollectionType.GET_BY_VALUE.name());
        }
    }

    @Slf4j
    class RemoveByValue implements CollectionOperation<UniqueListContext, Iterator<KeyValue>, Object> {

        @Override
        public List<KeyValue> execute(UniqueListContext context, Iterator<KeyValue> records) {
            List<KeyValue> list = new ArrayList<>();
            try {
                Column[] columns = context.columns;
                int[] indexes = new int[columns.length];
                for (int i = 0; i < columns.length; i++) {
                    indexes[i] = context.definition.getColumnIndexOfValue(columns[i].name);
                }
                while (records.hasNext()) {
                    KeyValue keyValue = records.next();
                    try {
                        Object[] objects = context.dingoValueCodec().decode(keyValue.getValue(), indexes);
                        for (int i = 0; i < objects.length; i++) {
                            CopyOnWriteArrayList<?> value = new CopyOnWriteArrayList<>((List<?>) objects[i]);
                            for (Object v : value) {
                                if (v.equals(context.value.getObject())) {
                                    value.remove(v);
                                }
                            }
                            objects[i] = value;
                        }
                        byte[] bytes =
                            context.dingoValueCodec().encode(keyValue.getValue(), objects, indexes);
                        keyValue.setValue(bytes);
                        list.add(keyValue);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            } catch (Exception e) {
                log.error("Failed to delete element based on value", e);
            }
            return list;
        }
    }

}
