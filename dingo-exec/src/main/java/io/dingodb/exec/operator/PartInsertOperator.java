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

package io.dingodb.exec.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.table.TableId;
import io.dingodb.common.table.TupleMapping;
import io.dingodb.common.table.TupleSchema;

import javax.annotation.Nonnull;

@JsonTypeName("insert")
@JsonPropertyOrder({"table", "part", "schema", "keyMapping", "output"})
public final class PartInsertOperator extends PartModifyOperator {
    @JsonCreator
    public PartInsertOperator(
        @JsonProperty("table") TableId tableId,
        @JsonProperty("part") Object partId,
        @JsonProperty("schema") TupleSchema schema,
        @JsonProperty("keyMapping") TupleMapping keyMapping
    ) {
        super(tableId, partId, schema, keyMapping);
    }

    @Override
    public void init() {
        super.init();
    }

    @Override
    public synchronized boolean push(int pin, @Nonnull Object[] tuple) {
        if (part.insert(tuple)) {
            count++;
        }
        return true;
    }
}
