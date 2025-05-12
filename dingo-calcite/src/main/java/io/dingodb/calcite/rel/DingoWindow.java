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

package io.dingodb.calcite.rel;

import io.dingodb.calcite.visitor.DingoRelVisitor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.reflect.Type;
import java.util.List;

public class DingoWindow extends Window implements DingoRel {

    public DingoWindow(
        RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelNode input,
        List<RexLiteral> constants, RelDataType rowType, List<Group> groups
    ) {
        super(cluster, traitSet, hints, input, constants, rowType, groups);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new DingoWindow(this.getCluster(), traitSet, this.hints, sole(inputs), constants, rowType, groups);
    }

    public static class WindowRelInputGetter
        implements RexToLixTranslator.InputGetter {
        private final Expression row;
        private final PhysType rowPhysType;
        private final int actualInputFieldCount;
        private final List<Expression> constants;

        public WindowRelInputGetter(Expression row,
                                     PhysType rowPhysType, int actualInputFieldCount,
                                     List<Expression> constants) {
            this.row = row;
            this.rowPhysType = rowPhysType;
            this.actualInputFieldCount = actualInputFieldCount;
            this.constants = constants;
        }

        @Override public Expression field(BlockBuilder list, int index, Type storageType) {
            if (index < actualInputFieldCount) {
                Expression current = list.append("current", row);
                return rowPhysType.fieldReference(current, index, storageType);
            }
            return constants.get(index - actualInputFieldCount);
        }
    }


    @Override
    public <T> T accept(@NonNull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
