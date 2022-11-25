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

package io.dingodb.sdk.operation.result;

import io.dingodb.common.DingoOpResult;
import io.dingodb.sdk.operation.unit.numeric.NumberUnit;

public class ValueOpResult<V extends NumberUnit> implements DingoOpResult<V> {

    private V value;

    private int count;

    public ValueOpResult(V value) {
        this(value, 0);
    }

    public ValueOpResult(V value, int count) {
        this.value = value;
        this.count = count;
    }

    @Override
    public V getValue() {
        return (V) value;
    }

    public int getCount() {
        return count;
    }

    @Override
    public DingoOpResult merge(V that) {
        if (value != null) {
            value.merge(that);
        } else {
            value = that;
        }
        value = value == null ? that : (V) value.merge(that);
        return this;
    }
}
