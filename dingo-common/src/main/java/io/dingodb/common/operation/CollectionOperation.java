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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface CollectionOperation<D extends OperationContext, T, R> extends Executive<D, T, R> {

    @Slf4j
    class Size implements CollectionOperation<OperationContext, Iterator<KeyValue>, Object> {

        @Override
        public ExecutiveResult execute(OperationContext context, Iterator<KeyValue> records) {
            List<Map<String, Value>> result = new ArrayList<>();
            Column column = context.columns[0];
            int index = context.definition.getColumnIndexOfValue(column.name);
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    Collection value =
                        (Collection) context.dingoValueCodec().decode(keyValue.getValue(), new int[] {index})[0];
                    result.add(Collections.singletonMap(column.name, Value.get(value.size())));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return new ExecutiveResult(result, true, CollectionType.SIZE.name());
        }
    }

    class GetByIndex implements CollectionOperation<ListContext, Iterator<KeyValue>, Object> {

        @Override
        public ExecutiveResult execute(ListContext context, Iterator<KeyValue> records) {
            List<Map<String, Value>> result = new ArrayList<>();
            Column column = context.columns[0];
            int index = context.definition.getColumnIndexOfValue(column.name);
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    List list = (List) context.dingoValueCodec().decode(keyValue.getValue(), new int[]{index})[0];
                    Object value = list.get(context.index);
                    result.add(Collections.singletonMap(column.name, Value.get(value)));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return new ExecutiveResult(result, true, CollectionType.GET_BY_INDEX.name());
        }
    }

    class GetByIndexRange implements CollectionOperation<ListContext, Iterator<KeyValue>, Object> {

        @Override
        public ExecutiveResult execute(ListContext context, Iterator<KeyValue> records) {

            return null;
        }
    }

    class GetAll implements CollectionOperation<OperationContext, Iterator<KeyValue>, Object> {

        @Override
        public ExecutiveResult execute(OperationContext context, Iterator<KeyValue> records) {
            List<Map<String, Value>> result = new ArrayList<>();
            Column column = context.columns[0];
            int index = context.definition.getColumnIndexOfValue(column.name);

            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    Collection value =
                        (Collection) context.dingoValueCodec().decode(keyValue.getValue(), new int[] {index})[0];
                    result.add(Collections.singletonMap(column.name, Value.get(value)));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return new ExecutiveResult(result, true, CollectionType.GET_ALL.name());
        }
    }

    class Set implements CollectionOperation<ListContext, Object, Object> {

        @Override
        public Object execute(ListContext context, Object second) {
            return null;
        }
    }

    class Clear implements CollectionOperation<OperationContext, Iterator<KeyValue>, Object> {

        @Override
        public List<KeyValue> execute(OperationContext context, Iterator<KeyValue> records) {
            List<KeyValue> list = new ArrayList<>();
            Column column = context.columns[0];
            int index = context.definition.getColumnIndexOfValue(column.name);
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    Collection value =
                        (Collection) context.dingoValueCodec().decode(keyValue.getValue(), new int[]{index})[0];

                    value.clear();
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

    class RemoveByIndex implements CollectionOperation<ListContext, Iterator<KeyValue>, Object> {

        @Override
        public List<KeyValue> execute(ListContext context, Iterator<KeyValue> records) {
            List<KeyValue> list = new ArrayList<>();
            Column column = context.columns[0];
            int index = context.definition.getColumnIndexOfValue(column.name);
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    List value = (List) context.dingoValueCodec().decode(keyValue.getValue(), new int[]{index})[0];
                    value.remove(context.index);
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

    // map
    class Put implements CollectionOperation<MapContext, Iterator<KeyValue>, Object> {

        @Override
        public List<KeyValue> execute(MapContext context, Iterator<KeyValue> records) {
            List<KeyValue> list = new ArrayList<>();
            Column column = context.columns[0];
            int index = context.definition.getColumnIndexOfValue(column.name);
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    Map value = (Map) context.dingoValueCodec().decode(keyValue.getValue(), new int[]{index})[0];

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

    class RemoveByKey implements CollectionOperation<MapContext, Iterator<KeyValue>, Object> {

        @Override
        public List<KeyValue> execute(MapContext context, Iterator<KeyValue> records) {
            List<KeyValue> list = new ArrayList<>();
            Column column = context.columns[0];
            int index = context.definition.getColumnIndexOfValue(column.name);
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    Map value = (Map) context.dingoValueCodec().decode(keyValue.getValue(), new int[]{index})[0];
                    value.remove(context.key.getObject());

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

    class GetByKey implements CollectionOperation<MapContext, Iterator<KeyValue>, Object> {

        @Override
        public ExecutiveResult execute(MapContext context, Iterator<KeyValue> records) {
            List<Map<String, Value>> result = new ArrayList<>();
            Column column = context.columns[0];
            int index = context.definition.getColumnIndexOfValue(column.name);

            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    Map value =
                        (Map) context.dingoValueCodec().decode(keyValue.getValue(), new int[] {index})[0];
                    result.add(Collections.singletonMap(column.name, Value.get(value.get(context.key.getObject()))));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return new ExecutiveResult(result, true, CollectionType.GET_BY_KEY.name());
        }
    }

    // unique list
    class GetByValue implements CollectionOperation<UniqueListContext, Iterator<KeyValue>, Object> {

        @Override
        public ExecutiveResult execute(UniqueListContext context, Iterator<KeyValue> records) {
            List<Map<String, Value>> result = new ArrayList<>();
            Column column = context.columns[0];
            int index = context.definition.getColumnIndexOfValue(column.name);
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    java.util.Set value =
                        (java.util.Set) context.dingoValueCodec().decode(keyValue.getValue(), new int[]{index})[0];
                    for (Object obj : value) {
                        if (obj.equals(context.value.getObject())) {
                            result.add(Collections.singletonMap(column.name, Value.get(obj)));
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return new ExecutiveResult(result, true, CollectionType.GET_BY_VALUE.name());
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
