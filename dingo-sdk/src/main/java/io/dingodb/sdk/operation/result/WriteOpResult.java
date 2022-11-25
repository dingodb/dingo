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
import io.dingodb.common.store.KeyValue;

import java.util.Iterator;

public class WriteOpResult implements DingoOpResult {

    private Iterator<KeyValue> value;

    public WriteOpResult(Iterator<KeyValue> value) {
        this.value = value;
    }

    @Override
    public Iterator<KeyValue> getValue() {
        return value;
    }

    @Override
    public DingoOpResult merge(Object value) {
        return null;
    }
}
