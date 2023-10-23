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
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;

@JsonTypeName("delete")
@JsonPropertyOrder({"table", "part", "schema", "schemaVersion", "keyMapping", "output"})
public final class PartDeleteOperator extends PartModifyOperator {
    @JsonCreator
    public PartDeleteOperator(
        @JsonProperty("table") CommonId tableId,
        @JsonProperty("part") CommonId partId,
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("schemaVersion") int schemaVersion,
        @JsonProperty("keyMapping") TupleMapping keyMapping
    ) {
        super(tableId, partId, schema, schemaVersion, keyMapping);
    }

    @Override
    public void init() {
        super.init();
    }

    @Override
    protected boolean pushTuple(Object[] tuple) {
        if (part.remove(tuple)) {
            count++;
        }
        return true;
    }
}
