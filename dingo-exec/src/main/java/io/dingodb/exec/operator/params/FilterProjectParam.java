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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import lombok.Getter;

@Getter
public abstract class FilterProjectParam extends AbstractParams {

    @JsonProperty("tableId")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    protected final CommonId tableId;
    @JsonProperty("schema")
    protected final DingoType schema;
    @JsonProperty("filter")
    protected SqlExpr filter;
    @JsonProperty("selection")
    protected TupleMapping selection;
    @JsonProperty("keyMapping")
    protected final TupleMapping keyMapping;

    public FilterProjectParam(
        CommonId tableId,
        DingoType schema,
        SqlExpr filter,
        TupleMapping selection,
        TupleMapping keyMapping
    ) {
        super();
        this.tableId = tableId;
        this.schema = schema;
        this.filter = filter;
        this.selection = selection;
        this.keyMapping = keyMapping;
    }

    @Override
    public void init(Vertex vertex) {
        if (filter != null) {
            if (selection != null) {
                filter.compileIn(schema.select(selection), vertex.getParasType());
            } else {
                filter.compileIn(schema, vertex.getParasType());
            }
        }
    }

    @Override
    public void setParas(Object[] paras) {
        super.setParas(paras);
        if (filter != null) {
            filter.setParas(paras);
        }
    }
}
