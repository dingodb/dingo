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

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;

import java.util.Collections;


@Slf4j
class DingoInitializerExpressionFactory extends NullInitializerExpressionFactory {
    static DingoInitializerExpressionFactory INSTANCE = new DingoInitializerExpressionFactory();
    private final DingoParser parser;

    private DingoInitializerExpressionFactory() {
        parser = new DingoParser(new DingoParserContext(DingoRootSchema.DEFAULT_SCHEMA_NAME));
    }

    private SqlNode validateExprWithRowType(RelDataType rowType, SqlNode expr) {
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
        RelDataTypeFactory typeFactory = parser.getContext().getTypeFactory();
        CalciteCatalogReader catalogReader = SqlValidatorUtil.createSingleTableCatalogReader(
            true,
            tableName,
            typeFactory,
            rowType
        );
        SqlValidator validator = SqlValidatorUtil.newValidator(
            parser.getSqlValidator().getOperatorTable(),
            catalogReader,
            typeFactory,
            DingoParser.VALIDATOR_CONFIG
        );
        final SqlSelect select = (SqlSelect) validator.validate(select0);
        SqlNodeList selectList = select.getSelectList();
        return selectList.get(0);
    }

    @Override
    public ColumnStrategy generationStrategy(RelOptTable table, int column) {
        DingoTable dingoTable = DingoTable.dingo(table);
        return dingoTable.getTableDefinition().getColumnStrategy(column);
    }

    @Override
    public RexNode newColumnDefaultValue(RelOptTable table, int column, InitializerContext context) {
        DingoTable dingoTable = DingoTable.dingo(table);
        String defaultValue = dingoTable.getTableDefinition().getColumn(column).getDefaultValue();
        if (defaultValue == null) {
            return super.newColumnDefaultValue(table, column, context);
        }
        RelDataType rowType = table.getRowType();
        SqlNode sqlNode = context.parseExpression(DingoParser.PARSER_CONFIG, defaultValue);
        /*
        Should call the following, but it is not available to validate by our own config in Calcite, so call our
        alternatives.
        NOTE: the type is the table type, not the type of this column.
        sqlNode = context.validateExpression(table.getRowType(), sqlNode);
         */
        sqlNode = validateExprWithRowType(rowType, sqlNode);
        RexBuilder rexBuilder = parser.getCluster().getRexBuilder();
        RelDataType targetType = table.getRowType().getFieldList().get(column).getType();
        if (sqlNode.getKind() == SqlKind.LITERAL && ((SqlLiteral) sqlNode).getValue() == null) {
            return rexBuilder.makeNullLiteral(targetType);
        }
        RexNode rex = context.convertExpression(sqlNode);
        if (!rex.getType().equals(targetType)) {
            return rexBuilder.makeCast(targetType, rex, true);
        }
        return rex;
    }
}
