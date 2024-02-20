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

import io.dingodb.calcite.fun.DingoInferTypes;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.SqlTypeFamily;

import static org.apache.calcite.sql.type.OperandTypes.family;
import static org.apache.calcite.sql.type.SqlAppointReturnTypeInference.FLOAT;

public class SqlIPDistanceOperator extends SqlBinaryOperator {

    /**
     * Creates a SqlBinaryOperator.
     *
     * @param name                 Name of operator
     * @param kind                 Kind
     */
    public SqlIPDistanceOperator(String name, SqlKind kind) {
        super(
            name,
            kind,
            24,
            true,
            FLOAT,
            DingoInferTypes.FLOAT,
            family(SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY)
        );
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        writer.keyword(call.operand(0).toString());
        writer.keyword("<*>");
        writer.keyword(call.operand(1).toString());
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.FUNCTION;
    }
}
