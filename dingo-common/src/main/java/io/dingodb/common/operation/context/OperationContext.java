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

import io.dingodb.common.codec.DingoCodec;
import io.dingodb.common.codec.KeyValueCodec;
import io.dingodb.common.operation.Column;
import io.dingodb.common.operation.filter.DingoFilter;
import io.dingodb.common.table.DingoKeyValueCodec;
import io.dingodb.common.table.TableDefinition;

public abstract class OperationContext {

    public final Column[] columns;

    public TableDefinition definition;

    public DingoFilter filter;

    public byte[] primaryStartKey;
    public byte[] primaryEndKey;

    public OperationContext(Column[] columns) {
        this.columns = columns;
    }

    public OperationContext definition(TableDefinition definition) {
        this.definition = definition;
        return this;
    }

    public OperationContext startKey(byte[] primaryStartKey) {
        this.primaryStartKey = primaryStartKey;
        return this;
    }

    public OperationContext endKey(byte[] primaryEndKey) {
        this.primaryEndKey = primaryEndKey;
        return this;
    }

    public void filter(DingoFilter filter) {
        this.filter = filter;
    }

    public DingoCodec dingoValueCodec() {
        return new DingoCodec(definition.getDingoSchemaOfValue());
    }

    public DingoCodec dingoKeyCodec() {
        return new DingoCodec(definition.getDingoSchemaOfKey());
    }

    public KeyValueCodec keyValueCodec() {
        return new DingoKeyValueCodec(definition.getDingoType(), definition.getKeyMapping());
    }
}
