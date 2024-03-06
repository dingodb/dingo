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
import io.dingodb.common.CoprocessorV2;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.DingoCompileContext;
import io.dingodb.exec.expr.DingoRelConfig;
import io.dingodb.exec.utils.SchemaWrapperUtils;
import io.dingodb.expr.coding.CodingFlag;
import io.dingodb.expr.coding.RelOpCoder;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.json.RelOpDeserializer;
import io.dingodb.expr.rel.json.RelOpSerializer;
import io.dingodb.expr.runtime.type.TupleType;
import lombok.Getter;
import lombok.Setter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@JsonTypeName("scanRel")
@JsonPropertyOrder({
    "tableId",
    "schema",
    "keyMapping",
    "outputSchema",
    "rel",
})
public class ScanWithRelOpParam extends ScanParam {
    @JsonProperty("outSchema")
    private final DingoType outputSchema;
    @JsonProperty("pushDown")
    private final boolean pushDown;

    @Getter
    private final transient DingoRelConfig config;

    @Getter
    @JsonProperty("rel")
    @JsonSerialize(using = RelOpSerializer.class)
    @JsonDeserialize(using = RelOpDeserializer.class)
    private RelOp relOp;

    @Getter
    @Setter
    private transient CoprocessorV2 coprocessor;

    public ScanWithRelOpParam(
        CommonId tableId,
        @NonNull DingoType schema,
        TupleMapping keyMapping,
        @NonNull RelOp relOp,
        DingoType outputSchema,
        boolean pushDown
    ) {
        super(tableId, schema, keyMapping);
        this.relOp = relOp;
        this.outputSchema = outputSchema;
        this.pushDown = pushDown;
        coprocessor = null;
        config = new DingoRelConfig();
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        relOp = relOp.compile(new DingoCompileContext(
            (TupleType) schema.getType(),
            (TupleType) vertex.getParasType().getType()
        ), config);
        if (pushDown) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            if (RelOpCoder.INSTANCE.visit(relOp, os) == CodingFlag.OK) {
                List<Integer> selection = IntStream.range(0, schema.fieldCount())
                    .boxed()
                    .collect(Collectors.toList());
                // TODO
                TupleMapping outputKeyMapping = TupleMapping.of(new int[]{});
                coprocessor = CoprocessorV2.builder()
                    .originalSchema(SchemaWrapperUtils.buildSchemaWrapper(schema, keyMapping, tableId.seq))
                    .resultSchema(SchemaWrapperUtils.buildSchemaWrapper(outputSchema, outputKeyMapping, tableId.seq))
                    .selection(selection)
                    .relExpr(os.toByteArray())
                    .build();
            }
        }
    }

    public KeyValueCodec getPushDownCodec() {
        // TODO
        TupleMapping outputKeyMapping = TupleMapping.of(new int[]{});
        return CodecService.getDefault().createKeyValueCodec(outputSchema, outputKeyMapping);
    }

    @Override
    public void setParas(Object[] paras) {
        super.setParas(paras);
        config.getEvalContext().setParas(paras);
    }
}
