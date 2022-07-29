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

import io.dingodb.common.operation.executive.AddExecutive;
import io.dingodb.common.operation.executive.CountExecutive;
import io.dingodb.common.operation.executive.MaxExecutive;
import io.dingodb.common.operation.executive.MinExecutive;
import io.dingodb.common.operation.executive.SumExecutive;

public enum NumericType implements OperationType {
    // agg calc
    ADD(new AddExecutive(), true),
    MAX(new MaxExecutive(), false),
    MIN(new MinExecutive(), false),
    SUM(new SumExecutive(), false),
    COUNT(new CountExecutive(), false),
    ;

    public final transient Executive executive;
    public final boolean isWriteable;

    NumericType(Executive executive, boolean isWriteable) {
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
