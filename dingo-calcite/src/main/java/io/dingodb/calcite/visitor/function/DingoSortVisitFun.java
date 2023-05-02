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

import io.dingodb.calcite.rel.DingoSort;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.Location;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Operator;
import io.dingodb.exec.base.Output;
import io.dingodb.exec.operator.SortOperator;
import io.dingodb.exec.operator.data.SortCollation;
import io.dingodb.exec.operator.data.SortDirection;
import io.dingodb.exec.operator.data.SortNullDirection;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rex.RexLiteral;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.dingodb.calcite.rel.DingoRel.dingo;

public class DingoSortVisitFun {
    @NonNull
    public static Collection<Output> visit(
        Job job,
        IdGenerator idGenerator,
        Location currentLocation,
        DingoJobVisitor dingoJobVisitor,
        @NonNull DingoSort rel
    ) {
        Collection<Output> inputs = dingo(rel.getInput()).accept(dingoJobVisitor);
        return DingoBridge.bridge(idGenerator, inputs, new OperatorSupplier(rel));
    }

    @AllArgsConstructor
    static class OperatorSupplier implements Supplier<Operator> {

        final DingoSort rel;

        @Override
        public Operator get() {
            return new SortOperator(
                toSortCollation(rel.getCollation().getFieldCollations()),
                rel.fetch == null ? -1 : RexLiteral.intValue(rel.fetch),
                rel.offset == null ? 0 : RexLiteral.intValue(rel.offset)
            );
        }
    }

    private static List<SortCollation> toSortCollation(List<RelFieldCollation> collations) {
        return collations.stream().map(DingoSortVisitFun::toSortCollation).collect(Collectors.toList());
    }

    private static @NonNull SortCollation toSortCollation(@NonNull RelFieldCollation collation) {
        SortDirection d;
        switch (collation.direction) {
            case DESCENDING:
            case STRICTLY_DESCENDING:
                d = SortDirection.DESCENDING;
                break;
            default:
                d = SortDirection.ASCENDING;
                break;
        }
        SortNullDirection n;
        switch (collation.nullDirection) {
            case FIRST:
                n = SortNullDirection.FIRST;
                break;
            case LAST:
                n = SortNullDirection.LAST;
                break;
            default:
                n = SortNullDirection.UNSPECIFIED;
        }
        return new SortCollation(collation.getFieldIndex(), d, n);
    }
}
