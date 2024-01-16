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

import io.dingodb.calcite.schema.DingoRootSchema;
import io.dingodb.calcite.utils.TableUtils;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.meta.entity.Column;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collections;
import java.util.stream.Collectors;


@Slf4j
class DingoInitializerExpressionFactory extends NullInitializerExpressionFactory {
    static DingoInitializerExpressionFactory INSTANCE = new DingoInitializerExpressionFactory();

    private final SqlOperatorTable operatorTable;

    private DingoInitializerExpressionFactory() {
        operatorTable = new DingoParserContext(DingoRootSchema.DEFAULT_SCHEMA_NAME)
            .getSqlValidator()
            .getOperatorTable();
    }

    private SqlNode validateExprWithRowType(
        @NonNull InitializerContext context,
        RelDataType rowType,
        SqlNode expr
    ) {
        final String tableName = "_table_";
        final SqlSelect select0 = new SqlSelect(
            SqlParserPos.ZERO,
            null,
            new SqlNodeList(Collections.singletonList(expr), SqlParserPos.ZERO),
            new SqlIdentifier(tableName, SqlParserPos.ZERO),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        RexBuilder rexBuilder = context.getRexBuilder();
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        CalciteCatalogReader catalogReader = SqlValidatorUtil.createSingleTableCatalogReader(
            true,
            tableName,
            typeFactory,
            rowType
        );
        SqlValidator validator = SqlValidatorUtil.newValidator(
            operatorTable,
            catalogReader,
            typeFactory,
            DingoSqlValidator.CONFIG
        );
        final SqlSelect select = (SqlSelect) validator.validate(select0);
        return select.getSelectList().get(0);
    }

    @Override
    public ColumnStrategy generationStrategy(RelOptTable table, int column) {
        Column col = table.unwrap(DingoTable.class).getTable().getColumns().get(column);
        if (col.getDefaultValueExpr() != null || col.isAutoIncrement()) {
            return ColumnStrategy.DEFAULT;
        } else {
            return col.isNullable() ? ColumnStrategy.NULLABLE : ColumnStrategy.NOT_NULLABLE;
        }
    }

    @Override
    public RexNode newColumnDefaultValue(RelOptTable table, int column, InitializerContext context) {
        Column col = table.unwrap(DingoTable.class).getTable().getColumns().get(column);
        String defaultValue = col.getDefaultValueExpr();

        if (col.isAutoIncrement()) {
            defaultValue = "AutoIncrementFun("
                + "'" + ((DingoRelOptTable) table).getSchemaName() + "'"
                + ", "
                + "'" + ((DingoRelOptTable) table).getTableName() + "'"
                + ")";
        }
        if (StringUtils.isEmpty(defaultValue)) {
            return super.newColumnDefaultValue(table, column, context);
        }

        RelDataType rowType = table.getRowType();
        SqlNode sqlNode = context.parseExpression(DingoParser.PARSER_CONFIG, defaultValue);
        /*
        Should call the following, but it is not available to validate by our own config in Calcite, so call our
        alternatives.
        NOTE: the type is the table type, not the type of this column.
        */
        // sqlNode = context.validateExpression(table.getRowType(), sqlNode);
        sqlNode = validateExprWithRowType(context, rowType, sqlNode);
        RexBuilder rexBuilder = context.getRexBuilder();
        RelDataType targetType = table.getRowType().getFieldList().get(column).getType();
        RexNode rex;
        if (sqlNode.getKind() == SqlKind.LITERAL && ((SqlLiteral) sqlNode).getValue() == null) {
            rex = rexBuilder.makeNullLiteral(targetType);
        } else if (sqlNode.getKind() == SqlKind.MULTISET_VALUE_CONSTRUCTOR) {
            // context::convertExpression will try to find a sub query for multiset, which is not applicable, so use
            // our simplified version.
            assert sqlNode instanceof SqlCall;
            SqlCall call = (SqlCall) sqlNode;
            rex = rexBuilder.makeCall(
                SqlStdOperatorTable.MULTISET_VALUE,
                call.getOperandList().stream()
                    .map(context::convertExpression)
                    .collect(Collectors.toList())
            );
        } else {
            rex = context.convertExpression(sqlNode);
        }
        if (!rex.getType().equals(targetType) && targetType.getSqlTypeName() != SqlTypeName.ANY) {
            return rexBuilder.makeCast(targetType, rex, true);
        }
        return rex;
    }
}
