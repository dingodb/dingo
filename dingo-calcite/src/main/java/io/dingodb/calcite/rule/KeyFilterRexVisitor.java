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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Sarg;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class KeyFilterRexVisitor extends RexVisitorImpl<Set<Map<Integer, RexLiteral>>> {
    private final TableDefinition tableDefinition;
    private final int primaryKeyCount;
    private final RexBuilder rexBuilder;
    private boolean operandHasNonPrimaryKey = false;

    public KeyFilterRexVisitor(@NonNull TableDefinition tableDefinition, RexBuilder rexBuilder) {
        super(true);
        this.tableDefinition = tableDefinition;
        this.primaryKeyCount = tableDefinition.getKeyMapping().size();
        // reverse indices mapping from column index to key index.
        this.rexBuilder = rexBuilder;
    }

    // `null` means conflict.
    private static @Nullable Map<Integer, RexLiteral> merge(
        final Map<Integer, RexLiteral> item0,
        final @NonNull Map<Integer, RexLiteral> item1
    ) {
        for (Map.Entry<Integer, RexLiteral> entry : item1.entrySet()) {
            int index = entry.getKey();
            RexLiteral value0 = item0.get(index);
            RexLiteral value = entry.getValue();
            if (value0 == null) {
                item0.put(index, value);
            } else {
                if (value != value0) {
                    return null;
                }
            }
        }
        return item0;
    }

    private static @NonNull Set<Map<Integer, RexLiteral>> product(
        final @NonNull Set<Map<Integer, RexLiteral>> items,
        final Map<Integer, RexLiteral> item
    ) {
        items.removeIf(v -> merge(v, item) == null);
        return items;
    }

    private static @NonNull Set<Map<Integer, RexLiteral>> singleton(Map<Integer, RexLiteral> item) {
        Set<Map<Integer, RexLiteral>> items = new HashSet<>();
        items.add(item);
        return items;
    }

    /**
     * Check the keys set to see if a full scan needed.
     *
     * @param items key tuples to be checked
     * @return {@code true} means the primary columns are all set for each row
     *     {@code false} means any columns are not set for some rows, so full scan is needed
     */
    boolean checkKeyItems(Set<Map<Integer, RexLiteral>> items) {
        if (items == null) {
            return false;
        }
        for (Map<Integer, RexLiteral> item : items) {
            if (item.size() < primaryKeyCount) {
                return false;
            }
        }
        return true;
    }

    private Set<Map<Integer, RexLiteral>> product(
        final Set<Map<Integer, RexLiteral>> items0,
        final Set<Map<Integer, RexLiteral>> items1
    ) {
        if (items1 == null) {
            return items0;
        }
        if (items1.size() == 0) {
            items0.clear();
            return items0;
        } else if (items1.size() == 1) {
            for (Map<Integer, RexLiteral> item : items1) {
                return product(items0, item);
            }
        }
        Set<Map<Integer, RexLiteral>> newItems = new HashSet<>();
        for (Map<Integer, RexLiteral> item : items1) {
            Set<Map<Integer, RexLiteral>> t = copyItems(items0);
            newItems.addAll(product(t, item));
        }
        return newItems;
    }

    private Set<Map<Integer, RexLiteral>> copyItems(final @NonNull Set<Map<Integer, RexLiteral>> items) {
        return items.stream().map(item -> {
            Map<Integer, RexLiteral> map = new HashMap<>(primaryKeyCount);
            map.putAll(item);
            return map;
        }).collect(Collectors.toSet());
    }

    @Override
    public Set<Map<Integer, RexLiteral>> visitInputRef(@NonNull RexInputRef inputRef) {
        return singleton(makeItem(inputRef.getIndex(), rexBuilder.makeLiteral(true)));
    }

    // `null` means the RexNode is not related to primary column
    @Override
    public Set<Map<Integer, RexLiteral>> visitCall(@NonNull RexCall call) {
        Set<Map<Integer, RexLiteral>> items;
        List<RexNode> operands = call.getOperands();
        switch (call.getKind()) {
            case SEARCH:
                if (operands.get(0).isA(SqlKind.INPUT_REF) && operands.get(1).isA(SqlKind.LITERAL)) {
                    RexInputRef inputRef = (RexInputRef) operands.get(0);
                    RexLiteral literal = (RexLiteral) operands.get(1);
                    Sarg<?> value = (Sarg<?>) literal.getValue();
                    assert value != null;
                    if (value.isPoints()) {
                        items = new HashSet<>();
                        for (Range<?> range : value.rangeSet.asRanges()) {
                            Object s = range.lowerEndpoint();
                            items.add(makeItem(
                                inputRef.getIndex(),
                                rexBuilder.makeLiteral(s, inputRef.getType())
                            ));
                        }
                        return items;
                    }
                }
                return null;
            case OR:
                items = new HashSet<>();
                for (RexNode operand : operands) {
                    Set<Map<Integer, RexLiteral>> t = operand.accept(this);
                    if (t == null) {
                        return null;
                    }
                    items.addAll(t);
                }
                return items;
            case AND:
                items = singleton(makeItem());
                for (RexNode op : operands) {
                    Set<Map<Integer, RexLiteral>> t = op.accept(this);
                    items = product(items, t);
                }
                return items;
            case EQUALS:
                items = checkOperands(operands.get(0), operands.get(1));
                if (items == null) {
                    items = checkOperands(operands.get(1), operands.get(0));
                }
                return items;
            case NOT:
                if (operands.get(0).isA(SqlKind.INPUT_REF)) {
                    RexInputRef inputRef = (RexInputRef) operands.get(0);
                    return singleton(makeItem(inputRef.getIndex(), rexBuilder.makeLiteral(false)));
                }
                return null;
            default:
                return null;
        }
    }

    private @Nullable Set<Map<Integer, RexLiteral>> checkOperands(@NonNull RexNode op0, RexNode op1) {
        if (op0.isA(SqlKind.INPUT_REF) && op1.isA(SqlKind.LITERAL)) {
            RexInputRef inputRef = (RexInputRef) op0;
            RexLiteral literal = (RexLiteral) op1;
            return singleton(makeItem(inputRef.getIndex(), literal));
        }
        return null;
    }

    private @NonNull Map<Integer, RexLiteral> makeItem(int index, RexLiteral rexLiteral) {
        Map<Integer, RexLiteral> item = new HashMap<>(primaryKeyCount);
        if (tableDefinition.getColumn(index).isPrimary()) {
            item.put(index, rexLiteral);
        } else {
            this.operandHasNonPrimaryKey = true;
        }
        return item;
    }

    private @NonNull Map<Integer, RexLiteral> makeItem() {
        return new HashMap<>(primaryKeyCount);
    }

    /**
     * to check the input operands has non-primary columns.
     *
     * @return true: all input operands is in primary; else false
     */
    public boolean isOperandHasNotPrimaryKey() {
        return operandHasNonPrimaryKey;
    }
}
