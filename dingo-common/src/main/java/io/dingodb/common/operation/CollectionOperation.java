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
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface CollectionOperation<D extends OperationContext, T, R> extends Executive<D, T, R> {

    @Slf4j
    class Size implements CollectionOperation<OperationContext, Iterator<KeyValue>, Object> {

        @Override
        public DingoExecResult execute(OperationContext context, Iterator<KeyValue> records) {
            Map<String, Value> result = new HashMap<>();
            Column[] columns = context.columns;
            int[] indexes = new int[columns.length];
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    Object[] objects = context.dingoValueCodec().decode(keyValue.getValue(), indexes);
                    Map<String, Object> values = getCollectionSize(columns, objects);
                    Object[] key = context.keyValueCodec().decodeKey(keyValue.getKey());
                    result.put(Arrays.toString(key), Value.get(values));
                } catch (IOException e) {
                    return new DingoExecResult(false, "Size operation decode failed, " + e.getMessage());
                }
            }
            return new DingoExecResult(result, true, "OK", CollectionType.SIZE.name());
        }

        private static Map<String, Object> getCollectionSize(Column[] columns, Object[] objects) {
            Map<String, Object> values = new HashMap<>();
            for (int i = 0; i < objects.length; i++) {
                int size;
                if (objects[i] instanceof List) {
                    List list = (List) objects[i];
                    size = list.size();
                } else if (objects[i] instanceof java.util.Set) {
                    java.util.Set set = (java.util.Set) objects[i];
                    size = set.size();
                } else if (objects[i] instanceof Map) {
                    Map map = (Map) objects[i];
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
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    Object[] objects = context.dingoValueCodec().decode(keyValue.getValue(), indexes);
                    Map<String, Object> values = new HashMap<>();
                    for (int i = 0; i < objects.length; i++) {
                        List value = (List) objects[i];
                        values.put(columns[i].name, value.get(context.index));
                    }
                    Object[] key = context.keyValueCodec().decodeKey(keyValue.getKey());
                    result.put(Arrays.toString(key), Value.get(values));
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
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    Object[] objects = context.dingoValueCodec().decode(keyValue.getValue(), indexes);
                    Map<String, Object> values = new HashMap<>();
                    for (int i = 0; i < objects.length; i++) {
                        List list = (List) objects[i];
                        int size = context.index + context.count > list.size() ? list.size() : context.count;
                        List<Object> value = new ArrayList<>();
                        for (int j = context.index; j < size; j++) {
                            value.add(list.get(j));
                        }
                        values.put(columns[i].name, value);
                    }
                    Object[] key = context.keyValueCodec().decodeKey(keyValue.getKey());
                    result.put(Arrays.toString(key), Value.get(values));
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
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    Object[] objects = context.dingoValueCodec().decode(keyValue.getValue(), indexes);
                    Map<String, Object> values = new HashMap<>();
                    for (int i = 0; i < objects.length; i++) {
                        values.put(columns[i].name, objects[i]);
                    }
                    Object[] key = context.keyValueCodec().decodeKey(keyValue.getKey());
                    result.put(Arrays.toString(key), Value.get(values));
                } catch (IOException e) {
                    return new DingoExecResult(false, "Get all operation decode failed, " + e.getMessage());
                }
            }
            return new DingoExecResult(result, true, "OK", CollectionType.GET_ALL.name());
        }
    }

    class Set implements CollectionOperation<ListContext, Iterator<KeyValue>, Object> {

        @Override
        public List<KeyValue> execute(ListContext context, Iterator<KeyValue> records) {
            List<KeyValue> list = new ArrayList<>();
            Column[] columns = context.columns;
            int[] indexes = new int[columns.length];
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

    class Clear implements CollectionOperation<OperationContext, Iterator<KeyValue>, Object> {

        @Override
        public List<KeyValue> execute(OperationContext context, Iterator<KeyValue> records) {
            List<KeyValue> list = new ArrayList<>();
            Column[] columns = context.columns;
            int[] indexes = new int[columns.length];
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
            return list;
        }

        private static Object[] convert(Object[] objects) {
            for (Object object : objects) {
                if (object instanceof List) {
                    List list = (List) object;
                    list.clear();
                } else if (object instanceof java.util.Set) {
                    java.util.Set set = (java.util.Set) object;
                    set.clear();
                } else if (object instanceof Map) {
                    Map map = (Map) object;
                    map.clear();
                }
            }
            return objects;
        }
    }

    class RemoveByIndex implements CollectionOperation<ListContext, Iterator<KeyValue>, Object> {

        @Override
        public List<KeyValue> execute(ListContext context, Iterator<KeyValue> records) {
            List<KeyValue> list = new ArrayList<>();
            Column[] columns = context.columns;
            int[] indexes = new int[columns.length];
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    Object[] objects = context.dingoValueCodec().decode(keyValue.getValue(), indexes);
                    for (Object object : objects) {
                        List value = (List) object;
                        value.remove(context.index);
                    }
                    byte[] bytes =
                        context.dingoValueCodec().encode(keyValue.getValue(), objects, indexes);
                    keyValue.setValue(bytes);
                    list.add(keyValue);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return list;
        }
    }

    // map
    class Put implements CollectionOperation<MapContext, Iterator<KeyValue>, Object> {

        @Override
        public List<KeyValue> execute(MapContext context, Iterator<KeyValue> records) {
            List<KeyValue> list = new ArrayList<>();
            Column[] columns = context.columns;
            int[] indexes = new int[columns.length];
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    Object[] objects = context.dingoValueCodec().decode(keyValue.getValue(), indexes);
                    for (Object object : objects) {
                        Map value = (Map) object;
                        value.put(context.key.getObject(), context.value.getObject());
                    }
                    byte[] bytes =
                        context.dingoValueCodec().encode(keyValue.getValue(), objects, indexes);
                    keyValue.setValue(bytes);
                    list.add(keyValue);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return list;
        }
    }

    class RemoveByKey implements CollectionOperation<MapContext, Iterator<KeyValue>, Object> {

        @Override
        public List<KeyValue> execute(MapContext context, Iterator<KeyValue> records) {
            List<KeyValue> list = new ArrayList<>();
            Column[] columns = context.columns;
            int[] indexes = new int[columns.length];
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    Object[] objects = context.dingoValueCodec().decode(keyValue.getValue(), indexes);
                    for (Object object : objects) {
                        Map value = (Map) object;
                        value.remove(context.key.getObject());
                    }

                    byte[] bytes = context.dingoValueCodec().encode(keyValue.getValue(), objects, indexes);
                    keyValue.setValue(bytes);
                    list.add(keyValue);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return list;
        }
    }

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
                    // {col1=10, col2=20, col3=30}
                    Map<String, Object> values = new HashMap<>();
                    for (int i = 0; i < objects.length; i++) {
                        Map value = (Map) objects[i];
                        values.put(columns[i].name, value.get(context.key.getObject()));
                    }
                    Object[] key = context.keyValueCodec().decodeKey(keyValue.getKey());
                    result.put(Arrays.toString(key), Value.get(values));

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
                        java.util.Set value = (java.util.Set) objects[i];
                        for (Object obj : value) {
                            if (obj.equals(context.value.getObject())) {
                                values.put(columns[i].name, obj);
                            }
                        }
                    }
                    Object[] key = context.keyValueCodec().decodeKey(keyValue.getKey());
                    result.put(Arrays.toString(key), Value.get(values));
                } catch (IOException e) {
                    return new DingoExecResult(false, "Get by value operation decode failed, " + e.getMessage());
                }
            }
            return new DingoExecResult(result, true, "OK", CollectionType.GET_BY_VALUE.name());
        }
    }

    class RemoveByValue implements CollectionOperation<UniqueListContext, Iterator<KeyValue>, Object> {

        @Override
        public List<KeyValue> execute(UniqueListContext context, Iterator<KeyValue> records) {
            List<KeyValue> list = new ArrayList<>();
            Column column = context.columns[0];
            int index = context.definition.getColumnIndexOfValue(column.name);
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    java.util.Set value =
                        (java.util.Set) context.dingoValueCodec().decode(keyValue.getValue(), new int[]{index})[0];
                    for (Object obj : value) {
                        if (obj.equals(context.value.getObject())) {
                            value.remove(obj);
                        }
                    }
                    byte[] bytes =
                        context.dingoValueCodec().encode(keyValue.getValue(), new Object[]{value}, new int[]{index});
                    keyValue.setValue(bytes);
                    list.add(keyValue);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return list;
        }
    }

}
