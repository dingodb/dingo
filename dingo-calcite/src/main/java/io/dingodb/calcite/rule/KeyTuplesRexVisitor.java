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

package io.dingodb.calcite.rule;

import com.google.common.collect.Range;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.table.TupleMapping;
import io.dingodb.common.table.TupleSchema;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Sarg;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

class KeyTuplesRexVisitor extends RexVisitorImpl<Set<Object[]>> {
    private final TupleMapping keyMapping;
    private final TupleSchema keySchema;
    // reverse indices mapping from column index to key index.
    private final TupleMapping revKeyMapping;
    private boolean operandHasNonPrimaryKey = false;

    public KeyTuplesRexVisitor(@Nonnull TableDefinition tableDefinition) {
        super(true);
        keyMapping = tableDefinition.getKeyMapping();
        keySchema = tableDefinition.getTupleSchema(true);
        int columnsCount = tableDefinition.getColumnsCount();
        revKeyMapping = keyMapping.reverse(columnsCount);
    }

    // `null` means conflict.
    @Nullable
    private static Object[] merge(@Nonnull final Object[] tuple, final Object[] tuple1) {
        for (int i = 0; i < tuple.length; ++i) {
            if (tuple1[i] != null) {
                if (tuple[i] == null) {
                    tuple[i] = tuple1[i];
                } else {
                    if (!tuple1[i].equals(tuple[i])) {
                        return null;
                    }
                }
            }
        }
        return tuple;
    }

    private static Set<Object[]> copyTuples(@Nonnull final Set<Object[]> tuples) {
        return tuples.stream().map(t -> Arrays.copyOf(t, t.length)).collect(Collectors.toSet());
    }

    @Nonnull
    private static Set<Object[]> product(@Nonnull final Set<Object[]> tuples, final Object[] tuple) {
        tuples.removeIf(v -> merge(v, tuple) == null);
        return tuples;
    }

    private static Set<Object[]> product(final Set<Object[]> tuples, final Set<Object[]> tuples1) {
        if (tuples1 == null) {
            return tuples;
        }
        if (tuples1.size() == 0) {
            tuples.clear();
            return tuples;
        } else if (tuples1.size() == 1) {
            for (Object[] tuple : tuples1) {
                return product(tuples, tuple);
            }
        }
        Set<Object[]> newTuples = new HashSet<>();
        for (Object[] tuple : tuples1) {
            Set<Object[]> t = copyTuples(tuples);
            newTuples.addAll(product(t, tuple));
        }
        return newTuples;
    }

    private static void add(@Nonnull final Set<Object[]> tuples, Set<Object[]> tuples1) {
        tuples.addAll(tuples1);
    }

    private static void add(@Nonnull final Set<Object[]> tuples, Object[] tuple) {
        tuples.add(tuple);
    }

    // `null` means the RexNode is not related to primary column
    @Override
    public Set<Object[]> visitCall(@Nonnull RexCall call) {
        Set<Object[]> tuples;
        List<RexNode> operands = call.getOperands();
        switch (call.getKind()) {
            case SEARCH:
                if (operands.get(0).isA(SqlKind.INPUT_REF) && operands.get(1).isA(SqlKind.LITERAL)) {
                    RexInputRef inputRef = (RexInputRef) operands.get(0);
                    RexLiteral literal = (RexLiteral) operands.get(1);
                    Sarg<?> value = (Sarg<?>) literal.getValue();
                    assert value != null;
                    if (value.isPoints()) {
                        tuples = new HashSet<>();
                        for (Range<?> range : value.rangeSet.asRanges()) {
                            Object s = range.lowerEndpoint();
                            add(tuples, makeTuple(inputRef.getIndex(), s));
                        }
                        return tuples;
                    }
                }
                return null;
            case OR:
                tuples = new HashSet<>();
                for (RexNode operand : operands) {
                    Set<Object[]> t = operand.accept(this);
                    if (t == null) {
                        return null;
                    }
                    add(tuples, t);
                }
                return tuples;
            case AND:
                tuples = getOneEmpty();
                for (RexNode op : operands) {
                    Set<Object[]> t = op.accept(this);
                    tuples = product(tuples, t);
                }
                return tuples;
            case EQUALS:
                tuples = checkOperands(operands.get(0), operands.get(1));
                if (tuples == null) {
                    tuples = checkOperands(operands.get(1), operands.get(0));
                }
                return tuples;
            case NOT:
                if (operands.get(0).isA(SqlKind.INPUT_REF)) {
                    RexInputRef inputRef = (RexInputRef) operands.get(0);
                    return getOneWithEntry(inputRef.getIndex(), false);
                }
                return null;
            default:
                return super.visitCall(call);
        }
    }

    @Override
    public Set<Object[]> visitInputRef(@Nonnull RexInputRef inputRef) {
        return getOneWithEntry(inputRef.getIndex(), true);
    }

    @Nullable
    private Set<Object[]> checkOperands(@Nonnull RexNode op0, @Nonnull RexNode op1) {
        if (op0.isA(SqlKind.INPUT_REF) && op1.isA(SqlKind.LITERAL)) {
            RexInputRef inputRef = (RexInputRef) op0;
            RexLiteral literal = (RexLiteral) op1;
            return getOneWithEntry(inputRef.getIndex(), literal.getValue());
        }
        return null;
    }

    @Nonnull
    private Object[] makeTuple(int index, Object value) {
        Object[] tuple = new Object[keyMapping.size()];
        if (0 <= index && index < revKeyMapping.size()) {
            int i = revKeyMapping.get(index);
            if (i >= 0) {
                tuple[i] = keySchema.get(i).convert(value);
            } else {
                this.operandHasNonPrimaryKey = true;
            }
        }
        return tuple;
    }

    @Nonnull
    private Set<Object[]> getOneWithEntry(int index, Object value) {
        Set<Object[]> tuples = new HashSet<>();
        tuples.add(makeTuple(index, value));
        return tuples;
    }

    @Nonnull
    private Set<Object[]> getOneEmpty() {
        return getOneWithEntry(-1, null);
    }

    /**
     * to check the input operands has non primary columns.
     * @return true: all input operands is in primary; else false
     */
    public boolean isOperandHasNotPrimaryKey() {
        return operandHasNonPrimaryKey;
    }
}
