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

import io.dingodb.common.operation.compute.CollectionType;
import io.dingodb.common.operation.context.UniqueListContext;

public class UniqueListOperation {

    /**
     * Create unique list size operation.
     * Return size of unique list.
     */
    public static Operation size(Column column) {
        return new Operation(CollectionType.SIZE, new UniqueListContext(column));
    }

    /**
     * Create unique list clear operation.
     * Removes all items in unique list col.
     */
    public static Operation clear(Column column) {
        return new Operation(CollectionType.CLEAR, new UniqueListContext(column));
    }

    /**
     * Create unique list operation.
     * Set item value at specified value in unique list col.
     */
    public static Operation set(Value value, Column column) {
        UniqueListContext context = new UniqueListContext(value, column);
        // return new Operation(CollectionType)
        return null;
    }

    /**
     * Create unique list operation.
     * Return item at specified value in unique list col.
     */
    public static Operation getByValue(Value value, Column column) {
        UniqueListContext context = new UniqueListContext(value, column);
        return new Operation(CollectionType.GET_BY_VALUE, context);
    }

    public static Operation remove(Value value, Column column) {
        UniqueListContext context = new UniqueListContext(value, column);
        return new Operation(CollectionType.REMOVE_BY_VALUE, context);
    }
}
