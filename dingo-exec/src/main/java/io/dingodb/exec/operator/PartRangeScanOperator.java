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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.Iterators;
import io.dingodb.codec.CodecService;
import io.dingodb.common.AggregationOperator;
import io.dingodb.common.CommonId;
import io.dingodb.common.Coprocessor;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.Services;
import io.dingodb.exec.aggregate.AbstractAgg;
import io.dingodb.exec.aggregate.Agg;
import io.dingodb.exec.expr.ExprCodeType;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.table.PartInKvStore;
import io.dingodb.exec.utils.SchemaWrapperUtils;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@JsonTypeName("scan")
@JsonPropertyOrder({
    "table", "part", "schema", "keyMapping", "filter", "selection", "output",
    "startKey", "endKey", "includeStart", "includeEnd", "prefixScan"
})
public final class PartRangeScanOperator extends PartIteratorSourceOperator {
    @JsonProperty("startKey")
    private final byte[] startKey;
    @JsonProperty("endKey")
    private final byte[] endKey;
    @JsonProperty("includeStart")
    private final boolean includeStart;
    @JsonProperty("includeEnd")
    private final boolean includeEnd;
    @JsonProperty("aggKeys")
    private final TupleMapping aggKeys;
    @JsonProperty("aggList")
    @JsonSerialize(contentAs = AbstractAgg.class)
    private final List<Agg> aggList;
    @JsonProperty("outSchema")
    private final DingoType outputSchema;
    @JsonProperty("pushDown")
    private final boolean pushDown;

    private Coprocessor coprocessor = null;

    @JsonCreator
    public PartRangeScanOperator(
        @JsonProperty("table") CommonId tableId,
        @JsonProperty("part") CommonId partId,
        @JsonProperty("schema") DingoType schema,
        @JsonProperty("keyMapping") TupleMapping keyMapping,
        @JsonProperty("filter") SqlExpr filter,
        @JsonProperty("selection") TupleMapping selection,
        @JsonProperty("startKey") byte[] startKey,
        @JsonProperty("endKey") byte[] endKey,
        @JsonProperty("includeStart") boolean includeStart,
        @JsonProperty("includeEnd") boolean includeEnd,
        @JsonProperty("aggKeys") TupleMapping aggKeys,
        @JsonProperty("aggList") @JsonDeserialize(contentAs = AbstractAgg.class) List<Agg> aggList,
        @JsonProperty("outSchema") DingoType outSchema,
        @JsonProperty("pushDown") boolean pushDown
    ) {
        super(tableId, partId, schema, keyMapping, filter, selection);
        this.startKey = startKey;
        this.endKey = endKey;
        this.includeStart = includeStart;
        this.includeEnd = includeEnd;
        this.aggKeys = aggKeys;
        this.aggList = aggList;
        this.outputSchema = outSchema;
        this.pushDown = pushDown;
    }

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator() {
        Iterator<Object[]> iterator;
        DingoType realOutputSchema;
        if (coprocessor == null) {
            realOutputSchema = schema;
            iterator = part.scan(startKey, endKey, includeStart, includeEnd);
        } else {
            iterator = part.scan(startKey, endKey, includeStart, includeEnd, coprocessor);
            if (!coprocessor.getAggregations().isEmpty()) {
                realOutputSchema = outputSchema;
            } else {
                realOutputSchema = schema;
            }
        }
        if (log.isDebugEnabled()) {
            iterator = Iterators.filter(
                iterator,
                tuple -> {
                    log.debug("got tuple {}.", realOutputSchema.format(tuple));
                    return true;
                }
            );
        }
        return iterator;
    }

    @Override
    public void init() {
        super.init();
        if (pushDown) {
            TupleMapping outputKeyMapping = keyMapping;
            Coprocessor.CoprocessorBuilder builder = Coprocessor.builder();
            if (selection != null) {
                builder.selection(selection.stream().boxed().collect(Collectors.toList()));
                selection = null;
                outputKeyMapping = TupleMapping.of(new int[]{});
            }
            if (filter != null) {
                ExprCodeType ect = filter.getCoding(schema, getParasType());
                if (ect != null) {
                    builder.expression(ect.getCode());
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
            part = new PartInKvStore(
                Services.KV_STORE.getInstance(tableId, partId),
                CodecService.getDefault().createKeyValueCodec(tableId, outputSchema, outputKeyMapping)
            );
            return;
        }
        part = new PartInKvStore(
            Services.KV_STORE.getInstance(tableId, partId),
            CodecService.getDefault().createKeyValueCodec(tableId, schema, keyMapping)
        );
    }
}
