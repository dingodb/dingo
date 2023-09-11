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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.TupleType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.impl.OutputIml;
import io.dingodb.exec.partition.PartitionStrategy;
import io.dingodb.meta.MetaService;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

public class VectorPartitionOperator extends FanOutOperator {
    @JsonProperty("tableId")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private final CommonId tableId;
    @JsonProperty("strategy")
    private final PartitionStrategy<CommonId, byte[]> strategy;

    @JsonProperty("partIndices")
    @JsonSerialize(keyUsing = CommonId.JacksonKeySerializer.class)
    @JsonDeserialize(keyUsing = CommonId.JacksonKeyDeserializer.class)
    private Map<CommonId, Integer> partIndices;

    private Integer index;

    private final KeyValueCodec codec;

    @JsonCreator
    public VectorPartitionOperator(
        @JsonProperty("tableId") CommonId tableId,
        @JsonProperty("strategy") PartitionStrategy<CommonId, byte[]> strategy,
        @JsonProperty("indexId") CommonId indexId,
        @JsonProperty("index") Integer index,
        @JsonProperty("indexTableDefinition") TableDefinition td
    ) {
        super();
        this.tableId = tableId;
        this.strategy = strategy;
        this.index = index;
        DingoType dingoType = new LongType(false);
        TupleType tupleType = DingoTypeFactory.tuple(new DingoType[]{dingoType});
        TupleMapping outputKeyMapping = TupleMapping.of(
            new int[] {0}
        );
        this.codec = CodecService.getDefault().createKeyValueCodec(indexId, tupleType, outputKeyMapping);
    }


    @Override
    protected int calcOutputIndex(int pin, Object @NonNull [] tuple) {
        // extract vector id from tuple
        Long vectorId = (Long) tuple[index];
        Object[] record = new Object[] {vectorId};
        byte[] key = null;
        try {
            key = codec.encodeKey(record);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        CodecService.getDefault().setId(key, CommonId.EMPTY_TABLE);
        CommonId partId = strategy.calcPartId(key);

        return partIndices.get(partId);
    }

    public void createOutputs(
        @NonNull NavigableMap<ByteArrayUtils.ComparableByteArray,
        RangeDistribution> distributions
    ) {
        int size = distributions.size();
        outputs = new ArrayList<>(size);
        partIndices = new HashMap<>(size);
        for (RangeDistribution distribution : distributions.values()) {
            Output output = OutputIml.of(this);
            OutputHint hint = new OutputHint();
            hint.setLocation(MetaService.root().currentLocation());
            hint.setPartId(distribution.id());
            output.setHint(hint);
            outputs.add(output);
            partIndices.put(distribution.id(), outputs.size() - 1);
        }
    }
}
