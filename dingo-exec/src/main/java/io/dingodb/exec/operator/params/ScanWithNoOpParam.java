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
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.DingoRelConfig;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

@JsonTypeName("scan0")
@JsonPropertyOrder({
    "tableId",
    "schema",
    "keyMapping",
    "pushDown",
})
public class ScanWithNoOpParam extends AbstractParams {
    @Getter
    @JsonProperty("table")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    protected final CommonId tableId;
    @Getter
    @JsonProperty("schema")
    protected final DingoType schema;
    @Getter
    @JsonProperty("keyMapping")
    protected final TupleMapping keyMapping;
    @Getter
    @JsonProperty("pushDown")
    protected final boolean pushDown;

    protected final DingoRelConfig config;

    @Getter
    protected KeyValueCodec codec;

    public ScanWithNoOpParam(
        CommonId tableId,
        @NonNull DingoType schema,
        TupleMapping keyMapping,
        boolean pushDown
    ) {
        super(null, null);
        this.tableId = tableId;
        this.schema = schema;
        this.keyMapping = keyMapping;
        this.pushDown = pushDown;
        config = new DingoRelConfig();
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        codec = CodecService.getDefault().createKeyValueCodec(schema, keyMapping);
    }

    @Override
    public void setParas(Object[] paras) {
        config.getEvalContext().setParas(paras);
    }
}
