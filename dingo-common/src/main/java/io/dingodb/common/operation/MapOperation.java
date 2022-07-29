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

import io.dingodb.common.operation.CollectionType;
import io.dingodb.common.operation.Column;
import io.dingodb.common.operation.Operation;
import io.dingodb.common.operation.Value;
import io.dingodb.common.operation.context.MapContext;

public final class MapOperation {

    /**
     * Create map size operation.
     * Return size of map.
     */
    public static Operation size(Column column) {
        MapContext context = new MapContext(column);
        return new Operation(CollectionType.SIZE, context);
    }

    /**
     * Create map put operation.
     * Writes key/value item to map col.
     */
    public static Operation put(Value key, Value value, Column column) {
        MapContext context = new MapContext(key, value, column);
        return new Operation(CollectionType.PUT, context);
    }

    /**
     * Create map clear operation.
     * removes all items in map.
     */
    public static Operation clear(Column column) {
        MapContext context = new MapContext(column);
        return new Operation(CollectionType.CLEAR, context);
    }

    /**
     * Create map remove operation.
     * Removes map item identified by key.
     */
    public static Operation removeByKey(Value key, Column column) {
        MapContext context = new MapContext(key, column);
        return new Operation(CollectionType.REMOVE_BY_KEY, context);
    }

    /**
     * Create map get by key operation.
     * Selects map item identified by key.
     */
    public static Operation getByKey(Value key, Column column) {
        MapContext context = new MapContext(key, column);
        return new Operation(CollectionType.GET_BY_KEY, context);
    }



}
