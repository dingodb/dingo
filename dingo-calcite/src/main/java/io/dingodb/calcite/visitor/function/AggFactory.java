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

package io.dingodb.calcite.visitor.function;

import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.aggregate.Agg;
import io.dingodb.exec.aggregate.CountAgg;
import io.dingodb.exec.aggregate.CountAllAgg;
import io.dingodb.exec.aggregate.MaxAgg;
import io.dingodb.exec.aggregate.MinAgg;
import io.dingodb.exec.aggregate.Sum0Agg;
import io.dingodb.exec.aggregate.SumAgg;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.common.util.Utils.sole;

final class AggFactory {
    private AggFactory() {
    }

    static @NonNull Agg getAgg(SqlKind kind, @NonNull List<Integer> args, DingoType schema) {
        if (args.isEmpty() && kind == SqlKind.COUNT) {
            return new CountAllAgg();
        }
        int index = sole(args);
        switch (kind) {
            case COUNT:
                return new CountAgg(index);
            case SUM:
                return new SumAgg(index, schema.getChild(index));
            case SUM0:
                return new Sum0Agg(index, schema.getChild(index));
            case MIN:
                return new MinAgg(index, schema.getChild(index));
            case MAX:
                return new MaxAgg(index, schema.getChild(index));
            default:
                break;
        }
        throw new UnsupportedOperationException("Unsupported aggregation function \"" + kind + "\".");
    }

    static @NonNull TupleMapping getAggKeys(@NonNull ImmutableBitSet groupSet) {
        return TupleMapping.of(
            groupSet.asList().stream()
                .mapToInt(Integer::intValue)
                .toArray()
        );
    }

    static List<Agg> getAggList(@NonNull List<AggregateCall> aggregateCallList, DingoType schema) {
        return aggregateCallList.stream()
            .map(c -> AggFactory.getAgg(
                c.getAggregation().getKind(),
                c.getArgList(),
                schema
            ))
            .collect(Collectors.toList());
    }
}
