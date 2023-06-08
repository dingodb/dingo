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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.table.Part;

public abstract class PartIteratorSourceOperator extends FilterProjectSourceOperator {
    @JsonProperty("table")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    protected final CommonId tableId;
    @JsonProperty("part")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    protected final CommonId partId;
    @JsonProperty("keyMapping")
    protected final TupleMapping keyMapping;

    protected Part part;

    protected PartIteratorSourceOperator(
        CommonId tableId,
        CommonId partId,
        DingoType schema,
        TupleMapping keyMapping,
        SqlExpr filter,
        TupleMapping selection
    ) {
        super(schema, filter, selection);
        this.tableId = tableId;
        this.partId = partId;
        this.keyMapping = keyMapping;
        OutputHint hint = new OutputHint();
        hint.setPartId(partId);
        output.setHint(hint);
    }
}
