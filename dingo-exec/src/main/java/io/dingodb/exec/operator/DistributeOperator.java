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
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.DistributionParam;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.NavigableMap;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public class DistributeOperator extends SoleOutOperator {
    public static final DistributeOperator INSTANCE = new DistributeOperator();

    private DistributeOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        context = context.copy();
        DistributionParam param = vertex.getParam();
        Object[] newTuple = tuple;
        if (tuple.length > param.getTable().columns.size()) {
            if (param.isRight()) {
                newTuple = Arrays.copyOfRange(tuple, param.getLeftLength(),
                    param.getLeftLength() + param.getTable().columns.size());
            } else {
                newTuple = Arrays.copyOfRange(tuple, 0, param.getTable().columns.size());
            }
        }
        IndexTable indexTable = param.getIndexTable();
        PartitionService ps = PartitionService.getService(
            Optional.ofNullable(param.getTable().getPartitionStrategy())
                .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
        CommonId partId;
        CommonId tablePartId = null;
        if (param.getTableId().type.code == CommonId.CommonType.INDEX.code
            && indexTable != null) {
            context.setIndexId(param.getTableId());
            context.setTableId(indexTable.primaryId);
            PartitionService indexPs = PartitionService.getService(
                Optional.ofNullable(indexTable.getPartitionStrategy())
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
            Object[] oldIndexTuple;
            if (param.isRight()) {
                oldIndexTuple = Arrays.copyOfRange(
                    tuple, param.getLeftLength(), param.getLeftLength() + param.getTable().columns.size());
            } else {
                oldIndexTuple = tuple;
            }
            Object[] indexTuple = new Object[indexTable.columns.size()];
            for (int i = 0; i < indexTable.getMapping().size(); i++) {
                int colIx;
                if ((colIx = indexTable.getMapping().get(i)) > -1) {
                    indexTuple[i] = oldIndexTuple[colIx];
                }
            }
            KeyValueCodec indexCodec = CodecService.getDefault()
                .createKeyValueCodec(indexTable.getCodecVersion(), indexTable.version,
                    indexTable.tupleType(), indexTable.keyMapping());
            partId = indexPs.calcPartId(indexTuple, wrap(indexCodec::encodeKey), param.getDistributions());
            NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distribution =
                MetaService.root().getRangeDistribution(param.getTable().tableId);
            tablePartId = ps.calcPartId(newTuple, wrap(param.getCodec()::encodeKey), distribution);
        } else {
            partId = ps.calcPartId(newTuple, wrap(param.getCodec()::encodeKey), param.getDistributions());
            context.setTableId(param.getTableId());
        }
        RangeDistribution distribution = RangeDistribution.builder().id(partId).build();
        context.setDistribution(distribution);
        context.setTablePartId(tablePartId);

        return vertex.getSoleEdge().transformToNext(context, tuple);
    }

    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        vertex.getSoleEdge().fin(fin);
    }
}
