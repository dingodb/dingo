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

import io.dingodb.codec.CodecService;
import io.dingodb.common.CommonId;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.VectorPartitionParam;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import org.checkerframework.checker.nullness.qual.NonNull;

public class VectorPartitionOperator extends FanOutOperator {
    public static final VectorPartitionOperator INSTANCE = new VectorPartitionOperator();

    private VectorPartitionOperator() {
    }

    @Override
    protected int calcOutputIndex(Context context, Object @NonNull [] tuple, Vertex vertex) {
        VectorPartitionParam param = vertex.getParam();
        // extract vector id from tuple
        Long vectorId = (Long) tuple[param.getIndex()];
        Object[] record = new Object[] {vectorId};
        byte[] key;
        key = param.getCodec().encodeKey(record);
        CodecService.getDefault().setId(key, CommonId.EMPTY_TABLE);
        CommonId partId = PartitionService.getService(
                Optional.ofNullable(param.getTable().getPartitionStrategy())
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
            .calcPartId(key, param.getDistributions());

        return param.getPartIndices().get(partId);
    }
}
