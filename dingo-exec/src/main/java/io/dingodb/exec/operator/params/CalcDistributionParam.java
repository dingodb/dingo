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

import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import lombok.Getter;

import java.util.NavigableMap;

@Getter
public class CalcDistributionParam extends AbstractParams {

    private final KeyValueCodec codec;
    private final TableDefinition tableDefinition;
    private final NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution;
    private PartitionService ps;

    public CalcDistributionParam(
        TableDefinition tableDefinition,
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution) {
        this.codec = CodecService.getDefault().createKeyValueCodec(tableDefinition);
        this.tableDefinition = tableDefinition;
        this.rangeDistribution = rangeDistribution;
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        ps = PartitionService.getService(
            Optional.ofNullable(tableDefinition.getPartDefinition())
                .map(PartitionDefinition::getFuncName)
                .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME)
        );
    }
}
