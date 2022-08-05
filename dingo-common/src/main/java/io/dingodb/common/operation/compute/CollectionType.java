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

package io.dingodb.common.operation.compute;

import io.dingodb.common.operation.CollectionOperation;
import io.dingodb.common.operation.OperationType;
import io.dingodb.common.operation.executive.Executive;

public enum CollectionType implements OperationType {
    // generic
    CLEAR(new CollectionOperation.Clear(), true),
    SIZE(new CollectionOperation.Size(), false),
    GET_ALL(new CollectionOperation.GetAll(), false),

    // list
    SET(new CollectionOperation.Set(), true),
    REMOVE_BY_INDEX(new CollectionOperation.RemoveByIndex(), true),
    GET_BY_INDEX(new CollectionOperation.GetByIndex(), false),
    GET_BY_INDEX_RANGE(new CollectionOperation.GetByIndexRange(), false),

    // unique list
    GET_BY_VALUE(new CollectionOperation.GetByValue(), false),
    REMOVE_BY_VALUE(new CollectionOperation.RemoveByValue(), true),

    // map
    PUT(new CollectionOperation.Put(), true),
    REMOVE_BY_KEY(new CollectionOperation.RemoveByKey(), true),
    GET_BY_KEY(new CollectionOperation.GetByKey(), false),
    ;

    public final transient Executive executive;
    public final boolean isWriteable;

    CollectionType(Executive executive, boolean isWriteable) {
        this.executive = executive;
        this.isWriteable = isWriteable;
    }

    @Override
    public Executive executive() {
        return this.executive;
    }

    @Override
    public boolean isWriteable() {
        return this.isWriteable;
    }

}
