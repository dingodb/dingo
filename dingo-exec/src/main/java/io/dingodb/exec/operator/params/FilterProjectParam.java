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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.DingoCompileContext;
import io.dingodb.exec.expr.DingoRelConfig;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.expr.common.type.TupleType;
import io.dingodb.expr.rel.RelOp;
import lombok.Getter;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY;
@Getter
@JsonAutoDetect(fieldVisibility = ANY)
public abstract class FilterProjectParam extends AbstractParams {

    @JsonProperty("tableId")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    public final CommonId tableId;
    @JsonProperty("schema")
    public final DingoType schema;
    @JsonProperty("schemaVersion")
    public final int schemaVersion;
    @JsonProperty("codecVersion")
    public final int codecVersion;
    @JsonProperty("filter")
    public SqlExpr filter;
    @JsonProperty("relOp")
    public RelOp relOp;
    @JsonProperty("selection")
    public TupleMapping selection;
    @JsonProperty("keyMapping")
    public final TupleMapping keyMapping;
    public final transient DingoRelConfig config;

    public FilterProjectParam(
        CommonId tableId,
        DingoType schema,
        int schemaVersion,
        SqlExpr filter,
        TupleMapping selection,
        TupleMapping keyMapping,
        int codecVersion
    ) {
        this(tableId, schema, schemaVersion, filter, selection, keyMapping, codecVersion, null);
    }

    public FilterProjectParam(
        CommonId tableId,
        DingoType schema,
        int schemaVersion,
        SqlExpr filter,
        TupleMapping selection,
        TupleMapping keyMapping,
        int codecVersion,
        RelOp relOp
    ) {
        super();
        this.tableId = tableId;
        this.schema = schema;
        this.schemaVersion = schemaVersion;
        this.filter = filter;
        this.selection = selection;
        this.keyMapping = keyMapping;
        this.codecVersion = codecVersion;
        this.relOp = relOp;
        this.config = new DingoRelConfig();
    }

    @Override
    public void init(Vertex vertex) {
        if (relOp != null) {
            TupleType tupleType = (TupleType) schema.getType();
            if (selection != null) {
                tupleType = (TupleType) schema.select(selection).getType();
            }
            relOp = relOp.compile(new DingoCompileContext(
                tupleType,
                (TupleType) vertex.getParasType().getType()
            ), config);
        }
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
        config.getEvalContext().setParas(paras);
    }
}

