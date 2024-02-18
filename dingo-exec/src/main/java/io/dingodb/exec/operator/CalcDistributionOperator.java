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

import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.RangeUtils;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.DistributionSourceParam;
import io.dingodb.partition.PartitionService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeSet;

@Slf4j
public class CalcDistributionOperator extends IteratorSourceOperator {
    public static final CalcDistributionOperator INSTANCE = new CalcDistributionOperator();

    private CalcDistributionOperator() {
    }

    @Override
    protected @NonNull Iterator<Object[]> createIterator(Vertex vertex) {
        DistributionSourceParam param = vertex.getParam();
        PartitionService ps = param.getPs();
        byte[] startKey = param.getStartKey();
        byte[] endKey = param.getEndKey();
        boolean withStart = param.isWithStart();
        boolean withEnd = param.isWithEnd();
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges = param.getRangeDistribution();
        NavigableSet<RangeDistribution> distributions;
        if (param.getFilter() != null || param.isNotBetween()) {
            if (param.isLogicalNot() || param.isNotBetween()) {
                distributions = new TreeSet<>(RangeUtils.rangeComparator(1));
                distributions.addAll(ps.calcPartitionRange(null, startKey, true, !withStart, ranges));
                distributions.addAll(ps.calcPartitionRange(endKey, null, !withEnd, true, ranges));
            } else {
                distributions = ps.calcPartitionRange(startKey, endKey, withStart, withEnd, ranges);
            }
        } else {
            distributions = ps.calcPartitionRange(startKey, endKey, withStart, withEnd, ranges);
        }

        return new ArrayList<>(distributions).stream().map(d -> new Object[]{d, param.getKeyTuple()}).iterator();
    }
}
