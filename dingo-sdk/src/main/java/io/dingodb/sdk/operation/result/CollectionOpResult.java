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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CollectionOpResult<V extends Iterator<Object[]>> implements DingoOpResult<V> {

    private Iterator<Object[]> value;

    public CollectionOpResult(Iterator<Object[]> value) {
        this.value = value;
    }

    @Override
    public V getValue() {
        return (V) value;
    }

    @Override
    public DingoOpResult<V> merge(V that) {
        // todo modify merge
        List<Object[]> list = new ArrayList<>();
        value.forEachRemaining(list::add);
        that.forEachRemaining(list::add);
        value = list.iterator();
        return this;
    }
}
