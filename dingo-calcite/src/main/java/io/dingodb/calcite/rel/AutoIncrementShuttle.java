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

import io.dingodb.calcite.DingoTable;
import io.dingodb.meta.entity.Column;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;

public class AutoIncrementShuttle implements RelShuttle {

    public static AutoIncrementShuttle INSTANCE = new AutoIncrementShuttle();

    @Override
    public RelNode visit(TableScan scan) {
        return null;
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
        return null;
    }

    @Override
    public RelNode visit(LogicalValues values) {
        return null;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        return null;
    }

    @Override
    public RelNode visit(LogicalCalc calc) {
        return null;
    }

    @Override
    public RelNode visit(LogicalProject project) {
        return null;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        return null;
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        return null;
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        return null;
    }

    @Override
    public RelNode visit(LogicalIntersect intersect) {
        return null;
    }

    @Override
    public RelNode visit(LogicalMinus minus) {
        return null;
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        return null;
    }

    @Override
    public RelNode visit(LogicalMatch match) {
        return null;
    }

    @Override
    public RelNode visit(LogicalSort sort) {
        return null;
    }

    @Override
    public RelNode visit(LogicalExchange exchange) {
        return null;
    }

    @Override
    public RelNode visit(LogicalTableModify modify) {
        return null;
    }

    @Override
    public RelNode visit(RelNode other) {
        if (other instanceof DingoTableModify) {
            DingoTableModify modify = (DingoTableModify) other;
            if (modify.isInsert() || modify.isUpdate()) {
                DingoTable table = modify.getTable().unwrap(DingoTable.class);
                boolean hasAutoIncrement = false;
                int autoIncrementColIndex = 0;
                for (Column columnDefinition : table.getTable().getColumns()) {
                    if (columnDefinition.isAutoIncrement()) {
                        hasAutoIncrement = true;
                        autoIncrementColIndex = table.getTable().getColumns().indexOf(columnDefinition);
                    }
                }
                if (hasAutoIncrement && other.getInputs().size() > 0) {
                    RelNode values = visitChildren(other);
                    modify.setHasAutoIncrement(true);
                    modify.setAutoIncrementColIndex(autoIncrementColIndex);
                    if (values instanceof DingoValues) {
                        DingoValues dingoValues = (DingoValues) values;
                        dingoValues.setHasAutoIncrement(true);
                        dingoValues.setAutoIncrementColIndex(autoIncrementColIndex);
                        dingoValues.setCommonId(table.getTableId());
                        return dingoValues;
                    }
                }
            }
            return null;
        } else if (other instanceof DingoValues) {
            return other;
        } else {
            if (other.getInputs().size() > 0) {
                return visitChildren(other);
            } else {
                return null;
            }
        }
    }

    protected RelNode visitChildren(RelNode rel) {
        for (Ord<RelNode> input : Ord.zip(rel.getInputs())) {
            rel = visitChild(rel, input.e);
        }
        return rel;
    }

    protected RelNode visitChild(RelNode parent, RelNode child) {
        RelNode child2 = child.accept(this);
        if (child2 instanceof DingoValues) {
            return child2;
        }
        return null;
    }
}
