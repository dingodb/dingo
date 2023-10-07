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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class DingoInferTypes {
    public static final SqlOperandTypeInference DECIMAL = allToBe(SqlTypeName.DECIMAL);
    public static final SqlOperandTypeInference DOUBLE = allToBe(SqlTypeName.DOUBLE);
    public static final SqlOperandTypeInference TIMESTAMP = allToBe(SqlTypeName.TIMESTAMP);
    public static final SqlOperandTypeInference VARCHAR = allToBe(SqlTypeName.VARCHAR);
    public static final SqlOperandTypeInference VARCHAR1024_INTEGER
        = explicit(SqlTypeName.VARCHAR, SqlTypeName.INTEGER);
    public static final SqlOperandTypeInference VARCHAR1024_INTEGER_INTEGER
        = explicit(SqlTypeName.VARCHAR, SqlTypeName.INTEGER, SqlTypeName.INTEGER);
    public static final SqlOperandTypeInference DATE_VARCHAR1024
        = explicit(SqlTypeName.DATE, SqlTypeName.VARCHAR);
    public static final SqlOperandTypeInference TIME_VARCHAR1024
        = explicit(SqlTypeName.TIME, SqlTypeName.VARCHAR);
    public static final SqlOperandTypeInference TIMESTAMP_VARCHAR1024
        = explicit(SqlTypeName.TIMESTAMP, SqlTypeName.VARCHAR);
    public static final SqlOperandTypeInference DATE_DATE
        = explicit(SqlTypeName.DATE, SqlTypeName.DATE);
    public static final SqlOperandTypeInference FLOAT
        = explicit(SqlTypeName.FLOAT, SqlTypeName.FLOAT);

    public static final SqlOperandTypeInference VARCHAR1024_VARCHAR1024_BOOLEAN
        = explicit(SqlTypeName.VARCHAR, SqlTypeName.VARCHAR, SqlTypeName.BOOLEAN);

    private DingoInferTypes() {
    }

    private static @NonNull SqlOperandTypeInference allToBe(final SqlTypeName sqlTypeName) {
        return (callBinding, returnType, operandTypes) -> {
            RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
            for (int i = 0; i < operandTypes.length; ++i) {
                operandTypes[i] = createType(typeFactory, sqlTypeName);
            }
        };
    }

    private static RelDataType createType(RelDataTypeFactory typeFactory, SqlTypeName sqlTypeName) {
        if (sqlTypeName == SqlTypeName.VARCHAR) {
            return typeFactory.createSqlType(sqlTypeName, 1024);
        } else {
            return typeFactory.createSqlType(sqlTypeName);
        }
    }

    private static @NonNull SqlOperandTypeInference explicit(final SqlTypeName... sqlTypeNames) {
        return (callBinding, returnType, operandTypes) -> {
            RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
            for (int i = 0; i < operandTypes.length; ++i) {
                operandTypes[i] = createType(typeFactory, sqlTypeNames[i]);
            }
        };
    }
}
