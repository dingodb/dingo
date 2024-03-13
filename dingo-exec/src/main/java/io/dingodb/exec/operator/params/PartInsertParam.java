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

package io.dingodb.exec.operator.params;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.meta.entity.Table;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
@JsonTypeName("insert")
@JsonPropertyOrder({"table", "schema", "keyMapping"})
public class PartInsertParam extends PartModifyParam {

    @JsonProperty("hasAutoInc")
    private final boolean hasAutoInc;

    @JsonProperty("autoIncColIdx")
    private final int autoIncColIdx;

    private List<Long> autoIncList = new ArrayList<>();

    public PartInsertParam(
        @JsonProperty("table") CommonId tableId,
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("keyMapping") TupleMapping keyMapping,
        Table table,
        @JsonProperty("hasAutoInc") boolean hasAutoInc,
        @JsonProperty("autoIncColIdx") int autoIncColIdx
    ) {
        super(tableId, schema, keyMapping, table);
        this.hasAutoInc = hasAutoInc;
        this.autoIncColIdx = autoIncColIdx;

    }

    public void inc() {
        count++;
    }
}
