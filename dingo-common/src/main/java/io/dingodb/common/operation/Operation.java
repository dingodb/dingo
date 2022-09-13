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

import io.dingodb.common.operation.compute.BasicType;
import io.dingodb.common.operation.compute.NumericType;
import io.dingodb.common.operation.context.BasicContext;
import io.dingodb.common.operation.context.OperationContext;

import java.io.Serializable;

public final class Operation implements Serializable {

    private static final long serialVersionUID = 8603290191302531535L;

    public final OperationType operationType;
    public final OperationContext operationContext;

    public Operation(OperationType operationType, OperationContext operationContext) {
        this.operationType = operationType;
        this.operationContext = operationContext;
    }

    public static Operation add(boolean useDefaultWhenNotExisted, Column... columns) {
        return new Operation(NumericType.ADD, new BasicContext(useDefaultWhenNotExisted, columns));
    }

    public static Operation max(Column... columns) {
        return new Operation(NumericType.MAX, new BasicContext(columns));
    }

    public static Operation min(Column... columns) {
        return new Operation(NumericType.MIN, new BasicContext(columns));
    }

    public static Operation sum(Column... columns) {
        return new Operation(NumericType.SUM, new BasicContext(columns));
    }

    public static Operation count(Column... columns) {
        return new Operation(NumericType.COUNT, new BasicContext(columns));
    }

    public static Operation update(Column... columns) {
        return new Operation(BasicType.UPDATE, new BasicContext(columns));
    }
}
