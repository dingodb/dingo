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

package io.dingodb.calcite;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;

class DingoInitializerExpressionFactory extends NullInitializerExpressionFactory {
    static DingoInitializerExpressionFactory INSTANCE = new DingoInitializerExpressionFactory();

    private DingoInitializerExpressionFactory() {
    }

    @Override
    public ColumnStrategy generationStrategy(RelOptTable table, int column) {
        DingoTable dingoTable = DingoTable.dingo(table);
        return dingoTable.getTableDefinition().getColumnStrategy(column);
    }

    @Override
    public RexNode newColumnDefaultValue(RelOptTable table, int column, InitializerContext context) {
        DingoTable dingoTable = DingoTable.dingo(table);
        Object defaultValue = dingoTable.getTableDefinition().getColumn(column).getDefaultValue();
        if (defaultValue != null) {
            RelDataType type = table.getRowType().getFieldList().get(column).getType();
            return context.getRexBuilder().makeLiteral(defaultValue, type);
        }
        return super.newColumnDefaultValue(table, column, context);
    }
}
