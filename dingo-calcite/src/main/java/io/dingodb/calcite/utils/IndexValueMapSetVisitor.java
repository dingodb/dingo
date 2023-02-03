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

import com.google.common.collect.Range;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Sarg;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IndexValueMapSetVisitor extends RexVisitorImpl<IndexValueMapSet<Integer, RexNode>> {
    private final RexBuilder rexBuilder;

    public IndexValueMapSetVisitor(RexBuilder rexBuilder) {
        super(true);
        this.rexBuilder = rexBuilder;
    }

    private static @NonNull IndexValueMapSet<Integer, RexNode> checkOperands(@NonNull RexNode op0, RexNode op1) {
        if (op0.isA(SqlKind.INPUT_REF) && ConstantTester.isConst(op1)) {
            RexInputRef inputRef = (RexInputRef) op0;
            return IndexValueMapSet.single(inputRef.getIndex(), op1);
        }
        return IndexValueMapSet.one();
    }

    @Override
    public IndexValueMapSet<Integer, RexNode> visitInputRef(@NonNull RexInputRef inputRef) {
        return IndexValueMapSet.single(inputRef.getIndex(), rexBuilder.makeLiteral(true));
    }

    // `null` means the RexNode is not related to primary column
    @Override
    public IndexValueMapSet<Integer, RexNode> visitCall(@NonNull RexCall call) {
        List<RexNode> operands = call.getOperands();
        switch (call.getKind()) {
            case SEARCH:
                if (operands.get(0).isA(SqlKind.INPUT_REF) && operands.get(1).isA(SqlKind.LITERAL)) {
                    RexInputRef inputRef = (RexInputRef) operands.get(0);
                    RexLiteral literal = (RexLiteral) operands.get(1);
                    Sarg<?> value = (Sarg<?>) literal.getValue();
                    assert value != null;
                    if (value.isPoints()) {
                        Set<Map<Integer, RexNode>> set = new HashSet<>();
                        for (Range<?> range : value.rangeSet.asRanges()) {
                            Object s = range.lowerEndpoint();
                            set.add(Collections.singletonMap(
                                inputRef.getIndex(),
                                rexBuilder.makeLiteral(s, inputRef.getType())
                            ));
                        }
                        return IndexValueMapSet.of(set);
                    }
                }
                break;
            case OR: {
                IndexValueMapSet<Integer, RexNode> o = IndexValueMapSet.zero();
                for (RexNode operand : operands) {
                    o = o.or(operand.accept(this));
                }
                return o;
            }
            case AND: {
                IndexValueMapSet<Integer, RexNode> o = IndexValueMapSet.one();
                for (RexNode operand : operands) {
                    o = o.and(operand.accept(this));
                }
                return o;
            }
            case EQUALS: {
                IndexValueMapSet<Integer, RexNode> o = checkOperands(operands.get(0), operands.get(1));
                if (o.isOne()) {
                    o = checkOperands(operands.get(1), operands.get(0));
                }
                return o;
            }
            case NOT:
                if (operands.get(0).isA(SqlKind.INPUT_REF)) {
                    RexInputRef inputRef = (RexInputRef) operands.get(0);
                    return IndexValueMapSet.single(inputRef.getIndex(), rexBuilder.makeLiteral(false));
                }
                break;
            default:
                break;
        }
        return IndexValueMapSet.one();
    }
}
