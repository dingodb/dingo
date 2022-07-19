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

package io.dingodb.sdk;

import io.dingodb.sdk.common.Value;
import io.dingodb.sdk.compute.Executive;
import io.dingodb.sdk.context.ListContext;
import io.dingodb.sdk.context.MapContext;
import io.dingodb.sdk.context.OperationContext;
import io.dingodb.sdk.context.UniqueListContext;

public interface CollectionOperation<D extends OperationContext, T, R> extends Executive<D, T, R> {

    // todo
    class Size implements CollectionOperation<OperationContext, Object, Object> {

        @Override
        public Value execute(OperationContext context, Object... second) {
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

    class GetByIndex implements CollectionOperation<ListContext, Object, Object> {

        @Override
        public Value execute(ListContext context, Object... second) {
            /*List list = (List) ((Value) first).value();
            Object result = list.get(second[0].index);
            return Value.get(result);*/
            return null;
        }
    }

    class GetByIndexRange implements CollectionOperation<OperationContext, Object, Object> {

        @Override
        public Value execute(OperationContext context, Object... second) {
            return null;
        }
    }

    class GetAll implements CollectionOperation<OperationContext, Object, Object> {

        @Override
        public Value execute(OperationContext context, Object... second) {
            return null;
        }
    }

    class Set implements CollectionOperation<ListContext, Object, Object> {

        @Override
        public Object execute(ListContext context, Object... second) {
            return null;
        }
    }

    class Clear implements CollectionOperation<OperationContext, Object, Object> {

        @Override
        public Value execute(OperationContext context, Object... second) {
            return null;
        }
    }

    class Remove implements CollectionOperation<ListContext, Object, Object> {

        @Override
        public Object execute(ListContext context, Object... second) {
            return null;
        }
    }

    // map
    class Put implements CollectionOperation<MapContext, Object, Object> {

        @Override
        public Object execute(MapContext context, Object... second) {
            return null;
        }
    }

    class RemoveByKey implements CollectionOperation<MapContext, Object, Object> {

        @Override
        public Object execute(MapContext context, Object... second) {
            return null;
        }
    }

    class GetByKey implements CollectionOperation<MapContext, Object, Object> {

        @Override
        public Object execute(MapContext context, Object... second) {
            return null;
        }
    }

    // unique list

    class GetByValue implements CollectionOperation<UniqueListContext, Object, Object> {

        @Override
        public Object execute(UniqueListContext first, Object... second) {
            return null;
        }
    }

    class RemoveByValue implements CollectionOperation<UniqueListContext, Object, Object> {

        @Override
        public Object execute(UniqueListContext context, Object... second) {
            return null;
        }
    }

}
