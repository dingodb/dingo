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

import io.dingodb.common.CommonId;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.converter.ValueConverter;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.PartitionParam;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

public final class PartitionOperator extends FanOutOperator {
    public static final PartitionOperator INSTANCE = new PartitionOperator();

    private PartitionOperator() {

    }

    @Override
    protected int calcOutputIndex(int pin, Object @NonNull [] tuple, Vertex vertex) {
        PartitionParam param = vertex.getParam();
        DingoType schema = param.getSchema();
        Object[] newTuple = (Object[]) schema.convertFrom(
            Arrays.copyOf(tuple, schema.fieldCount()),
            ValueConverter.INSTANCE
        );
        CommonId partId = PartitionService.getService(
                Optional.ofNullable(param.getTable().getPartitionStrategy())
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
            .calcPartId(newTuple, wrap(param.getCodec()::encodeKey), param.getDistributions());
        return param.getPartIndices().get(partId);
    }

}
