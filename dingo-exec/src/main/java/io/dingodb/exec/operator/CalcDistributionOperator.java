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
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.RangeUtils;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.DistributionSourceParam;
import io.dingodb.meta.MetaService;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeSet;

import static io.dingodb.common.util.Utils.calculatePrefixCount;

@Slf4j
public class CalcDistributionOperator extends IteratorSourceOperator {
    public static final CalcDistributionOperator INSTANCE = new CalcDistributionOperator();

    private CalcDistributionOperator() {
    }

    private static NavigableSet<RangeDistribution> getRangeDistributions(DistributionSourceParam param) {
        PartitionService ps = param.getPs();
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution
            = param.getRangeDistribution();
        byte[] startKey = param.getStartKey();
        byte[] endKey = param.getEndKey();
        boolean withStart = param.isWithStart();
        boolean withEnd = param.isWithEnd();
        NavigableSet<RangeDistribution> distributions;
        if (param.getFilter() != null || param.isNotBetween()) {
            if (param.isLogicalNot() || param.isNotBetween()) {
                distributions = new TreeSet<>(RangeUtils.rangeComparator(1));
                distributions.addAll(ps.calcPartitionRange(
                    null,
                    startKey,
                    true,
                    !withStart,
                    rangeDistribution));
                distributions.addAll(ps.calcPartitionRange(
                    endKey,
                    null,
                    !withEnd,
                    true,
                    rangeDistribution
                ));
                return distributions;
            }
        }
        return ps.calcPartitionRange(startKey, endKey, withStart, withEnd, rangeDistribution);
    }

    @Override
    protected @NonNull Iterator<Object[]> createIterator(Vertex vertex) {
        DistributionSourceParam param = vertex.getParam();
        PartitionService ps = param.getPs();
        NavigableSet<RangeDistribution> distributions = getRangeDistributions(param);
        Integer retry = Optional.mapOrGet(DingoConfiguration.instance().find("retry", int.class), __ -> __, () -> 30);
        while (retry-- > 0) {
            try {
                if (param.getKeyTuple() == null || distributions.size() > 1) {
                    return distributions.stream()
                        .map(d -> new Object[]{d, param.getKeyTuple()})
                        .iterator();
                } else {
                    Object[] keyTuple = param.getKeyTuple();
                    CommonId partId = ps.calcPartId(param.getCodec().encodeKeyPrefix(keyTuple, calculatePrefixCount(keyTuple)), param.getRangeDistribution());
                    RangeDistribution distribution = RangeDistribution.builder().id(partId).build();
                    return new ArrayList<>(distributions).stream()
                        .filter(d -> d.getId().equals(distribution.getId()))
                        .map(d -> new Object[]{d, param.getKeyTuple()})
                        .iterator();
                }
            } catch (RegionSplitException e) {
                LogUtils.error(log, e.getMessage());
                NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distribution =
                    MetaService.root().getRangeDistribution(param.getTd().getTableId());
                param.setRangeDistribution(distribution);
                distributions = getRangeDistributions(param);
            }
        }

        return new ArrayList<>(distributions).stream().map(d -> new Object[]{d, param.getKeyTuple()}).iterator();
    }
}
