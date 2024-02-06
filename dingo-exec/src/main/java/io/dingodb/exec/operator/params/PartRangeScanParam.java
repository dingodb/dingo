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
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.AggregationOperator;
import io.dingodb.common.CommonId;
import io.dingodb.common.Coprocessor;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.aggregate.AbstractAgg;
import io.dingodb.exec.aggregate.Agg;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.utils.SchemaWrapperUtils;
import lombok.Getter;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Getter
@JsonTypeName("scan")
@JsonPropertyOrder({
    "tableId", "part", "schema", "keyMapping", "filter", "selection", "prefixScan"
})
public class PartRangeScanParam extends FilterProjectParam {

    @JsonProperty("aggKeys")
    private final TupleMapping aggKeys;
    @JsonProperty("aggList")
    @JsonSerialize(contentAs = AbstractAgg.class)
    private final List<Agg> aggList;
    @JsonProperty("outSchema")
    private final DingoType outputSchema;
    @JsonProperty("pushDown")
    private final boolean pushDown;

    private transient Coprocessor coprocessor = null;
    private transient KeyValueCodec codec;

    public PartRangeScanParam(
        CommonId tableId,
        DingoType schema,
        TupleMapping keyMapping,
        SqlExpr filter,
        TupleMapping selection,
        TupleMapping aggKeys,
        List<Agg> aggList,
        DingoType outputSchema,
        boolean pushDown
    ) {
        super(tableId, schema, filter, selection, keyMapping);
        this.aggKeys = aggKeys;
        this.aggList = aggList;
        this.outputSchema = outputSchema;
        this.pushDown = pushDown;
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        if (pushDown) {
            TupleMapping outputKeyMapping = keyMapping;
            Coprocessor.CoprocessorBuilder builder = Coprocessor.builder();
            DingoType filterInputSchema;
            if (selection != null) {
                builder.selection(selection.stream().boxed().collect(Collectors.toList()));
                filterInputSchema = schema.select(selection);
                selection = null;
                outputKeyMapping = TupleMapping.of(new int[]{});
            } else {
                filterInputSchema = schema;
            }
            if (filter != null) {
                byte[] code = filter.getCoding(filterInputSchema, vertex.getParasType());
                if (code != null) {
                    builder.expression(code);
                    filter = null;
                }
            }
            if (aggList != null && !aggList.isEmpty()) {
                builder.groupBy(
                    aggKeys.stream()
                        .boxed()
                        .collect(Collectors.toList())
                );
                builder.aggregations(aggList.stream().map(
                    agg -> {
                        AggregationOperator.AggregationOperatorBuilder operatorBuilder = AggregationOperator.builder();
                        operatorBuilder.operation(agg.getAggregationType());
                        operatorBuilder.indexOfColumn(agg.getIndex());
                        return operatorBuilder.build();
                    }
                ).collect(Collectors.toList()));
                // Do not put group keys to codec key, for there may be null value.
                outputKeyMapping = TupleMapping.of(
                    IntStream.range(0, aggKeys.size()).boxed().collect(Collectors.toList())
                );
            }
            builder.originalSchema(SchemaWrapperUtils.buildSchemaWrapper(schema, keyMapping, tableId.seq));
            builder.resultSchema(SchemaWrapperUtils.buildSchemaWrapper(
                outputSchema, outputKeyMapping, tableId.seq
            ));
            coprocessor = builder.build();
            codec = CodecService.getDefault().createKeyValueCodec(outputSchema, outputKeyMapping);
            return;
        }
        codec = CodecService.getDefault().createKeyValueCodec(schema, keyMapping);
    }
}
