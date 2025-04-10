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

package io.dingodb.calcite.fun;

import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import static org.apache.calcite.sql.type.OperandTypes.family;

public class SqlIfFunction  extends SqlFunction {
    public static SqlIfFunction IF = new SqlIfFunction();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates the SqlIfFunction.
     */
    public SqlIfFunction() {
        super(
            "IF",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(SqlTypeName.ANY),
            InferTypes.ANY_NULLABLE,
            family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.ANY, SqlTypeFamily.ANY),
            SqlFunctionCategory.STRING);
    }

    @Override public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure) {
        return true;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.any();
    }
}
