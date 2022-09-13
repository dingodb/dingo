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

import io.dingodb.common.operation.OperationType;
import io.dingodb.common.operation.executive.Executive;
import io.dingodb.common.operation.executive.UpdateExecutive;

public enum BasicType implements OperationType {
    UPDATE(new UpdateExecutive(), true);

    public final transient Executive executive;
    public final boolean isWriteable;

    BasicType(Executive executive, boolean isWriteable) {
        this.executive = executive;
        this.isWriteable = isWriteable;
    }

    @Override
    public Executive executive() {
        return executive;
    }

    @Override
    public boolean isWriteable() {
        return isWriteable;
    }
}
