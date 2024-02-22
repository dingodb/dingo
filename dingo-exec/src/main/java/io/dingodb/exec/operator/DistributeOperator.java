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
import io.dingodb.common.config.DingoConfiguration;
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
import io.dingodb.store.api.transaction.exception.RegionSplitException;
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
        Integer retry = Optional.mapOrGet(DingoConfiguration.instance().find("retry", int.class), __ -> __, () -> 30);
        while (retry-- > 0) {
            try {
                Object[] newTuple = tuple;
                if (tuple.length > param.getTable().columns.size()) {
                    newTuple = Arrays.copyOfRange(tuple, 0, param.getTable().columns.size());
                }
                IndexTable indexTable = param.getIndexTable();
                PartitionService ps = PartitionService.getService(
                    Optional.ofNullable(param.getTable().getPartitionStrategy())
                        .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
                CommonId partId;
                if (param.getTableId().type.code == CommonId.CommonType.INDEX.code
                    && indexTable != null) {
                    context.setIndexId(param.getTableId());
                    ps = PartitionService.getService(
                        Optional.ofNullable(indexTable.getPartitionStrategy())
                            .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
                    Object[] indexTuple = new Object[indexTable.columns.size()];
                    for (int i = 0; i < indexTable.getMapping().size(); i++) {
                        indexTuple[i] = tuple[indexTable.getMapping().get(i)];
                    }
                    KeyValueCodec indexCodec = CodecService.getDefault()
                        .createKeyValueCodec(indexTable.tupleType(), indexTable.keyMapping());
                    partId = ps.calcPartId(indexTuple, wrap(indexCodec::encodeKey), param.getDistributions());
                } else {
                    partId = ps.calcPartId(newTuple, wrap(param.getCodec()::encodeKey), param.getDistributions());
                }
                RangeDistribution distribution = RangeDistribution.builder().id(partId).build();
                context.setDistribution(distribution);

                return vertex.getSoleEdge().transformToNext(context, tuple);
            } catch (RegionSplitException e) {
                log.error(e.getMessage());
                NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions =
                    MetaService.root().getRangeDistribution(param.getTableId());
                param.setDistributions(distributions);
            }
        }
        return true;
    }

    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        vertex.getSoleEdge().fin(fin);
    }
}
