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

package io.dingodb.exec.memory;

import io.dingodb.exec.operator.data.TupleWithJoinFlag;
import io.dingodb.exec.operator.params.HashJoinParam;
import io.dingodb.exec.tuple.TupleKey;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public interface Spiller {
    default void spill(ConcurrentHashMap<TupleKey, List<TupleWithJoinFlag>> hashMap, byte[] prefix, AtomicLong inc) {

    }

    default void spillHashMap(HashJoinParam hashJoinParam) {

    }

    void close(byte[] prefix);

    default Iterator<TupleWithJoinFlag> getValues(byte[] prefix, HashJoinParam hashJoinParam) {
        return null;
    }

    default void saveSingle(TupleKey tupleKey, TupleWithJoinFlag tupleWithJoinFlag, byte[] prefix) {

    }

    default void saveSingleKv(TupleKey tupleKey, TupleWithJoinFlag tupleWithJoinFlag, byte[] prefix, HashJoinParam hashJoinParam) {

    }
}
