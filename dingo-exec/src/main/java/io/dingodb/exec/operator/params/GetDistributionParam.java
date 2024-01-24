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
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.meta.entity.Table;
import lombok.Getter;

import java.util.List;
import java.util.NavigableMap;

@Getter
@JsonTypeName("getDistribution")
@JsonPropertyOrder({"keyMapping"})
public class GetDistributionParam extends SourceParam {

    private final List<Object[]> keyTuples;
    private final Table table;
    private final NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions;
    @JsonProperty("keyMapping")
    private final TupleMapping keyMapping;
    private transient KeyValueCodec codec;

    public GetDistributionParam(
        List<Object[]> keyTuples,
        TupleMapping keyMapping,
        Table table,
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions
    ) {
        this.keyTuples = keyTuples;
        this.keyMapping = keyMapping;
        this.distributions = distributions;
        this.table = table;
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        this.codec = CodecService.getDefault().createKeyValueCodec(table.tupleType(), table.keyMapping());

    }
}
