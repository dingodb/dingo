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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class Key {

    /**
     * Optional database for the key.
     */
    private final String database;

    /**
     * Required, user keys for with multiple columns.
     */
    public final List<Value> userKey;

    public Key(Value userKey) {
        this(Collections.singletonList(userKey));
    }

    public Key(List<Value> userKey) {
        this("dingo", userKey);
    }

    public Key(String database, List<Value> userKey) {
        this.database = database;
        this.userKey = userKey;
    }

    public List<Object> getUserKey() {
        return userKey.stream().map(Value::getObject).collect(Collectors.toList());
    }
}
