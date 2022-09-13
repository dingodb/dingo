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
import io.dingodb.common.operation.context.BasicContext;

public final class BasicOperation {

    private BasicOperation() {
    }

    // todo implement CURD in operation mode
    public static Operation put(Object[] records, Column... columns) {
        BasicContext context = new BasicContext(records, columns);
        return new Operation(null, context);
    }

    /**
     * Create operation to update the column.
     *
     * @param column column:value
     */
    public static Operation update(Column column) {
        return new Operation(BasicType.UPDATE, new BasicContext(column));
    }

}
