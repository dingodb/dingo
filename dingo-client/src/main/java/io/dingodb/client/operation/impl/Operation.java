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

package io.dingodb.client.operation.impl;

import io.dingodb.client.OperationContext;
import io.dingodb.client.common.IndexInfo;
import io.dingodb.client.common.TableInfo;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.utils.Any;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.NavigableSet;
import java.util.concurrent.atomic.AtomicReference;

public interface Operation {

    @Getter
    @EqualsAndHashCode
    @AllArgsConstructor
    class Task {
        private final DingoCommonId regionId;
        private final Any parameters;

        public <P> P parameters() {
            return parameters.getValue();
        }

    }

    @Getter
    @AllArgsConstructor
    class Fork {
        private final NavigableSet<Task> subTasks;
        private final boolean ignoreError;
        private final AtomicReference<Object> resultRef;

        public Fork(Object result, NavigableSet<Task> subTasks, boolean ignoreError) {
            this.resultRef = new AtomicReference<>(result);
            this.subTasks = subTasks;
            this.ignoreError = ignoreError;
        }

        public <R> R result() {
            return (R) resultRef.get();
        }
    }

    default Fork fork(Any parameters, TableInfo tableInfo) {
        throw new UnsupportedOperationException();
    }

    default Fork fork(OperationContext context, TableInfo tableInfo) {
        throw new UnsupportedOperationException();
    }

    default Fork fork(Any parameters, IndexInfo indexInfo) {
        throw new UnsupportedOperationException();
    }

    default Fork fork(OperationContext context, IndexInfo indexInfo) {
        throw new UnsupportedOperationException();
    }

    void exec(OperationContext context);

    <R> R reduce(Fork context);

    @RequiredArgsConstructor
    public static class VectorTuple<V> {
        public final int key;
        public final V value;
    }

}
