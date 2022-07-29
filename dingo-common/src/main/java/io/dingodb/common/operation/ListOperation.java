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
import io.dingodb.common.operation.context.ListContext;

public final class ListOperation {

    /**
     * Create list size operation.
     * Returns size of list.
     */
    public static Operation size(Column column) {
        return new Operation(CollectionType.SIZE, new ListContext(column));
    }

    /**
     * Create list clear operation.
     * Removes all items in list col.
     */
    public static Operation clear(Column column) {
        //
        return new Operation(CollectionType.CLEAR, new ListContext(column));
    }

    /**
     * Create list set operation.
     * Sets item value at specified index in list col.
     */
    public static Operation set(int index, Value value, Column column) {
        ListContext context = new ListContext(index, value, column);
        return new Operation(CollectionType.SET, context);
    }

    /**
     * Create list get operation.
     * Returns item at specified index in list col.
     */
    public static Operation getByIndex(int index, Column column) {
        //
        ListContext context = new ListContext(index, column);
        return new Operation(CollectionType.GET_BY_INDEX, context);
    }

    /**
     * Create list get range operation.
     * Returns 'count' items starting at specified index in list col.
     */
    public static Operation getByIndexRange(int index, int count, Column column) {
        //
        ListContext context = new ListContext(index, count, column);
        return new Operation(CollectionType.GET_BY_INDEX_RANGE, context);
    }

    /**
     * Create list remove operation.
     * removes item at specified index from list col.
     */
    public static Operation remove(int index, Column column) {
        ListContext context = new ListContext(index, column);
        return new Operation(CollectionType.REMOVE, context);
    }

    /**
     * Create list remove range operation.
     * removes 'count' items starting at specified index from list col.
     */
    public static Operation removeRange(int index, int count, Column column) {
        ListContext context = new ListContext(index, count, column);
        return new Operation(CollectionType.REMOVE, context);
    }
}
