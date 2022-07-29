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

import io.dingodb.common.operation.context.ListContext;
import io.dingodb.common.operation.context.MapContext;
import io.dingodb.common.operation.context.OperationContext;
import io.dingodb.common.operation.context.UniqueListContext;
import io.dingodb.common.store.KeyValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface CollectionOperation<D extends OperationContext, T, R> extends Executive<D, T, R> {

    // todo
    class Size implements CollectionOperation<OperationContext, Object, Object> {

        @Override
        public Value execute(OperationContext context, Object second) {
            /*Value value = second[0];
            if (value.getType() == ParticleType.LIST) {
                return Value.get(((List) value).size());
            }
            if (value.getType() == ParticleType.MAP) {
                return Value.get(((Map) value).size());
            }
            return Value.get(0);*/
            return null;
        }
    }

    class GetByIndex implements CollectionOperation<ListContext, Iterator<KeyValue>, Object> {

        @Override
        public ExecutiveResult execute(ListContext context, Iterator<KeyValue> records) {
            List<Map<String, Value>> result = new ArrayList<>();
            Map<String, Value> map = new HashMap<>();
            Column column = context.columns[0];
            int index = context.definition.getColumnIndexOfValue(column.name);
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                try {
                    List list = (List) context.dingoCodec().decode(keyValue.getValue(), new int[]{index})[0];
                    Object value = list.get(context.index);
                    map.put(column.name, Value.get(value));
                    result.add(map);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return new ExecutiveResult(result, true);
        }
    }

    class GetByIndexRange implements CollectionOperation<OperationContext, Object, Object> {

        @Override
        public Value execute(OperationContext context, Object second) {
            return null;
        }
    }

    class GetAll implements CollectionOperation<OperationContext, Object, Object> {

        @Override
        public Value execute(OperationContext context, Object second) {
            return null;
        }
    }

    class Set implements CollectionOperation<ListContext, Object, Object> {

        @Override
        public Object execute(ListContext context, Object second) {
            return null;
        }
    }

    class Clear implements CollectionOperation<OperationContext, Object, Object> {

        @Override
        public Value execute(OperationContext context, Object second) {
            return null;
        }
    }

    class Remove implements CollectionOperation<ListContext, Object, Object> {

        @Override
        public Object execute(ListContext context, Object second) {
            return null;
        }
    }

    // map
    class Put implements CollectionOperation<MapContext, Object, Object> {

        @Override
        public Object execute(MapContext context, Object second) {
            return null;
        }
    }

    class RemoveByKey implements CollectionOperation<MapContext, Object, Object> {

        @Override
        public Object execute(MapContext context, Object second) {
            return null;
        }
    }

    class GetByKey implements CollectionOperation<MapContext, Object, Object> {

        @Override
        public Object execute(MapContext context, Object second) {
            return null;
        }
    }

    // unique list

    class GetByValue implements CollectionOperation<UniqueListContext, Object, Object> {

        @Override
        public Object execute(UniqueListContext first, Object second) {
            return null;
        }
    }

    class RemoveByValue implements CollectionOperation<UniqueListContext, Object, Object> {

        @Override
        public Object execute(UniqueListContext context, Object second) {
            return null;
        }
    }

}
