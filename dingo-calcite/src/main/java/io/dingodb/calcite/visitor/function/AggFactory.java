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
import io.dingodb.exec.aggregate.GroupConcatAgg;
import io.dingodb.exec.aggregate.MaxAgg;
import io.dingodb.exec.aggregate.MinAgg;
import io.dingodb.exec.aggregate.Sum0Agg;
import io.dingodb.exec.aggregate.SumAgg;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
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

    static @NonNull Agg getAggFromCall(
        @NonNull AggregateCall aggCall,
        DingoType schema,
        @Nullable List<RexNode> projects) {

        SqlKind kind = aggCall.getAggregation().getKind();

        if (kind == SqlKind.LISTAGG) {
            return buildGroupConcatAgg(aggCall, projects);
        }
        return getAgg(kind, aggCall.getArgList(), schema);
    }

    private static @NonNull GroupConcatAgg buildGroupConcatAgg(
        @NonNull AggregateCall aggCall,
        @Nullable List<RexNode> projects) {

        List<Integer> argList = aggCall.getArgList();

        // argList[0] is the index of the column to be spliced
        int exprIndex = argList.isEmpty() ? 0 : argList.get(0);

        // Parse SEPARATOR
        // Calcite places the separator literal in the project expression corresponding to argList[1]
        String separator = resolveSeparator(argList, projects);

        // Parse ORDER BY multi-column information from AggregateCall.collation
        List<Integer> orderByIndices = new ArrayList<>();
        List<Boolean> orderByAscending = new ArrayList<>();
        for (RelFieldCollation fc : aggCall.getCollation().getFieldCollations()) {
            orderByIndices.add(fc.getFieldIndex());
            // DESCENDING → false, other directions are ASC → true
            orderByAscending.add(fc.getDirection() != RelFieldCollation.Direction.DESCENDING);
        }

        boolean distinct = aggCall.isDistinct();

        return new GroupConcatAgg(exprIndex, orderByIndices, orderByAscending, separator, distinct);
    }

    private static String resolveSeparator(
        @NonNull List<Integer> argList,
        @Nullable List<RexNode> projects) {

        if (argList.size() < 2) {
            return ",";
        }
        int sepIdx = argList.get(1);
        if (projects != null && sepIdx < projects.size()) {
            RexNode sepNode = projects.get(sepIdx);
            if (sepNode instanceof RexLiteral) {
                Object val = ((RexLiteral) sepNode).getValue2();
                if (val != null) {
                    return val.toString();
                }
            }
        }
        return ",";
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

    static List<Agg> getAggList(
        @NonNull List<AggregateCall> aggregateCallList,
        DingoType schema,
        @Nullable List<RexNode> projects) {
        return aggregateCallList.stream()
            .map(c -> AggFactory.getAggFromCall(c, schema, projects))
            .collect(Collectors.toList());
    }
}
