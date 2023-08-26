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

package org.apache.calcite.sql2rel;

import io.dingodb.calcite.DingoParserContext;
import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.grammar.SqlUserDefinedOperators;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.fun.SqlArrayValueConstructor;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SqlVectorOperator extends SqlFunction implements SqlTableFunction {

    public static void register(DingoParserContext context) {
        StandardConvertletTable.INSTANCE.registerOp(SqlUserDefinedOperators.VECTOR,
            (cx, call) -> {
                final TranslatableTable table = ((SqlVectorOperator) call.getOperator())
                    .getTable(context, call.operand(0).toString());
                DingoTable dingoTable = (DingoTable) table;

                final RelDataType rowType = dingoTable.getRowType(cx.getTypeFactory());
                RexBuilder rexBuilder = new RexBuilder(cx.getTypeFactory());

                List<SqlNode> parameters = call.getOperandList();
                List<RexNode> parameterList = new ArrayList<>();
                for (int i = 0; i < parameters.size(); i++) {
                    SqlNode sqlNode = parameters.get(i);
                    if (sqlNode instanceof SqlIdentifier) {
                        RexLiteral literal = rexBuilder.makeLiteral(sqlNode.toString());
                        parameterList.add(literal);
                    } else if (sqlNode instanceof SqlNumericLiteral) {
                        RexLiteral literal = rexBuilder.makeLiteral(((SqlNumericLiteral) sqlNode)
                            .intValue(false), new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER));
                        parameterList.add(literal);
                    } else if (sqlNode instanceof SqlBasicCall) {
                        RexNode rexNode = rexBuilder.makeCall(new SqlArrayValueConstructor(),
                            ((SqlCall) sqlNode).getOperandList().stream()
                                .map(o ->
                                    rexBuilder.makeLiteral(((SqlNumericLiteral) o).bigDecimalValue(),
                                        new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.FLOAT)))
                                .collect(Collectors.toList()));
                        parameterList.add(rexNode);
                    }
                }

                return rexBuilder.makeCall(rowType, call.getOperator(), parameterList);
            });
    }

    /**
     * Creates a SqlVectorOperator.
     *
     * @param name        Operator name
     */
    public SqlVectorOperator(String name, SqlKind kind) {
        super(
            name,
            kind,
            ReturnTypes.ARG0,
            InferTypes.VARCHAR_1024,
            OperandTypes.CURSOR,
            SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION
        );
    }

    @Override public SqlReturnTypeInference getRowTypeInference() {
        return this::inferRowType;
    }

    private RelDataType inferRowType(SqlOperatorBinding callBinding) {
        final RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
        final TranslatableTable table = getTable(null, null);
        return table.getRowType(typeFactory);
    }

    private TranslatableTable getTable(DingoParserContext context, String tableName) {
        return  (TranslatableTable) context.getDefaultSchema().getTable(tableName, false).getTable();
    }

    @Override
    public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
        return super.createCall(null, pos, super.createCall(functionQualifier, pos, operands));
    }

}
