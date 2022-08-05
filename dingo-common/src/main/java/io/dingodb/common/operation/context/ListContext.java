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

package io.dingodb.common.operation.context;

import io.dingodb.common.operation.Column;
import io.dingodb.common.operation.Value;

public class ListContext extends OperationContext {

    public final int index;
    public final int count;
    public final Value value;

    public ListContext(Column... columns) {
        super(columns);
        this.index = 0;
        this.count = 0;
        this.value = Value.getAsNull();
    }

    public ListContext(int index, Column... columns) {
        super(columns);
        this.index = index;
        this.count = 0;
        this.value = Value.getAsNull();
    }

    public ListContext(int index, Value value, Column... columns) {
        super(columns);
        this.index = index;
        this.value = value;
        this.count = 0;
    }

    public ListContext(int index, int count, Column... columns) {
        super(columns);
        this.index = index;
        this.count = count;
        this.value = Value.getAsNull();
    }

}
