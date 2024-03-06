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
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

@Slf4j
@Getter
@JsonTypeName("reduceAgg")
@JsonPropertyOrder({"rel", "schema"})
public class ReduceRelOpParam extends AbstractParams {
    @JsonProperty("schema")
    private final DingoType schema;

    @Getter
    private final transient DingoRelConfig config;

    @JsonProperty("rel")
    @JsonSerialize(using = RelOpSerializer.class)
    @JsonDeserialize(using = RelOpDeserializer.class)
    private RelOp relOp;

    public ReduceRelOpParam(RelOp relOp, DingoType schema) {
        super();
        this.relOp = relOp;
        this.schema = schema;
        config = new DingoRelConfig();
    }

    @Override
    public void init(@NonNull Vertex vertex) {
        relOp = relOp.compile(new DingoCompileContext(
            (TupleType) schema.getType(),
            (TupleType) vertex.getParasType().getType()
        ), config);
    }

    public synchronized void reduce(Object[] tuple) {
    }
}
