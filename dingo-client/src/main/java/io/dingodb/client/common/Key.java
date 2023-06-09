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

package io.dingodb.client.common;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class Key {

    public static Key EMPTY = new Key(Collections.emptyList());

    /**
     * Required, user keys for with multiple columns.
     */
    public final List<Value> userKey;

    public final boolean columnOrder;

    public Key(Value userKey) {
        this(Collections.singletonList(userKey));
    }

    public Key(List<Value> userKey) {
        this(userKey, false);
    }

    public Key(Value userKey, boolean columnOrder) {
        this(Collections.singletonList(userKey), columnOrder);
    }

    public Key(List<Value> userKey, boolean columnOrder) {
        this.userKey = userKey;
        this.columnOrder = columnOrder;
    }

    @Deprecated
    public Key(String database, List<Value> userKey) {
        this(userKey);
    }

    @Deprecated
    public Key(String database, List<Value> userKey, boolean columnOrder) {
        this(userKey, columnOrder);
    }

    public static Key of(boolean columnOrder, Object... userKey) {
        return new Key(Arrays.stream(userKey).map(Value::get).collect(Collectors.toList()), columnOrder);
    }

    public static Key of(Object... userKey) {
        return new Key(Arrays.stream(userKey).map(Value::get).collect(Collectors.toList()));
    }

    public List<Object> getUserKey() {
        return userKey.stream().map(Value::getObject).collect(Collectors.toList());
    }
}
