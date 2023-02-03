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

package io.dingodb.calcite.utils;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.checkerframework.checker.nullness.qual.NonNull;

public class ConstantTester implements RexVisitor<Boolean> {
    static final ConstantTester INSTANCE = new ConstantTester();

    public static boolean isConst(@NonNull RexNode rexNode) {
        return rexNode.accept(INSTANCE);
    }

    @Override
    public Boolean visitLiteral(RexLiteral literal) {
        return true;
    }

    @Override
    public Boolean visitInputRef(RexInputRef inputRef) {
        return false;
    }

    @Override
    public Boolean visitLocalRef(RexLocalRef localRef) {
        return false;
    }

    @Override
    public Boolean visitOver(RexOver over) {
        return false;
    }

    @Override
    public Boolean visitSubQuery(RexSubQuery subQuery) {
        return false;
    }

    @Override
    public Boolean visitTableInputRef(RexTableInputRef ref) {
        return false;
    }

    @Override
    public Boolean visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        return false;
    }

    @Override
    public Boolean visitCorrelVariable(RexCorrelVariable correlVariable) {
        // Correlating variables change when there is an internal restart.
        // Not good enough for our purposes.
        return false;
    }

    @Override
    public Boolean visitDynamicParam(RexDynamicParam dynamicParam) {
        return false;
    }

    @Override
    public Boolean visitCall(@NonNull RexCall call) {
        // Constant if operator meets the following conditions:
        // 1. It is deterministic;
        // 2. All its operands are constant.
        return call.getOperator().isDeterministic()
            && RexVisitorImpl.visitArrayAnd(this, call.getOperands());
    }

    @Override
    public Boolean visitRangeRef(RexRangeRef rangeRef) {
        return false;
    }

    @Override
    public Boolean visitFieldAccess(@NonNull RexFieldAccess fieldAccess) {
        // "<expr>.FIELD" is constant iff "<expr>" is constant.
        return fieldAccess.getReferenceExpr().accept(this);
    }
}
