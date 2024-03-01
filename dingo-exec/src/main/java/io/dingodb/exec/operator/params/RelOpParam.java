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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.DingoCompileContext;
import io.dingodb.exec.expr.DingoRelConfig;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.json.RelOpDeserializer;
import io.dingodb.expr.rel.json.RelOpSerializer;
import io.dingodb.expr.runtime.type.TupleType;
import lombok.Getter;

@JsonTypeName("rel")
@JsonPropertyOrder({
    "schema",
    "rel",
})
public class RelOpParam extends AbstractParams {
    @Getter
    @JsonProperty("schema")
    protected final DingoType schema;
    protected final DingoRelConfig config;
    @Getter
    @JsonProperty("rel")
    @JsonSerialize(using = RelOpSerializer.class)
    @JsonDeserialize(using = RelOpDeserializer.class)
    private RelOp relOp;

    public RelOpParam(
        DingoType schema,
        RelOp relOp
    ) {
        super();
        this.schema = schema;
        this.relOp = relOp;
        config = new DingoRelConfig();
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        relOp = relOp.compile(new DingoCompileContext(
            (TupleType) schema.getType(),
            (TupleType) vertex.getParasType().getType()
        ), config);
    }

    @Override
    public void setParas(Object[] paras) {
        config.getEvalContext().setParas(paras);
    }
}
