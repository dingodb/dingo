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

package io.dingodb.sdk.context;

import io.dingodb.sdk.common.Column;
import io.dingodb.sdk.common.Value;

public class MapContext extends OperationContext {

    public final Value key;
    public final Value value;

    public MapContext(Column... columns) {
        super(columns);
        this.key = Value.getAsNull();
        this.value = Value.getAsNull();
    }

    public MapContext(Value key, Column... columns) {
        super(columns);
        this.key = key;
        this.value = Value.getAsNull();
    }

    public MapContext(Value key, Value value, Column... columns) {
        super(columns);
        this.key = key;
        this.value = value;
    }
}
