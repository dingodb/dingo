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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.dingodb.exec.fun.number.FormatFun;
import io.dingodb.exec.fun.number.PowFun;
import io.dingodb.exec.fun.string.ConcatFun;
import io.dingodb.exec.fun.string.LTrimFun;
import io.dingodb.exec.fun.string.LeftFun;
import io.dingodb.exec.fun.string.LocateFun;
import io.dingodb.exec.fun.string.MidFun;
import io.dingodb.exec.fun.string.RTrimFun;
import io.dingodb.exec.fun.string.RepeatFun;
import io.dingodb.exec.fun.string.ReverseFun;
import io.dingodb.exec.fun.string.RightFun;
import io.dingodb.exec.fun.time.DateDiffFun;
import io.dingodb.exec.fun.time.DateFormatFun;
import io.dingodb.exec.fun.time.FromUnixTimeFun;
import io.dingodb.exec.fun.time.TimeFormatFun;
import io.dingodb.exec.fun.time.TimestampFormatFun;
import io.dingodb.exec.fun.time.UnixTimestampEvaluators;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Slf4j
public class DingoOperatorTable implements SqlOperatorTable {
    private static DingoOperatorTable instance;

    private final Multimap<String, SqlFunction> funMap;

    private DingoOperatorTable() {
        funMap = HashMultimap.create();
    }

    public static synchronized DingoOperatorTable instance() {
        if (instance == null) {
            instance = new DingoOperatorTable();
            instance.init();
        }
        return instance;
    }

    private void init() {
        // alias of std function
        funMap.put("LCASE", SqlStdOperatorTable.LOWER);
        funMap.put("UCASE", SqlStdOperatorTable.UPPER);
        funMap.put("NOW", SqlStdOperatorTable.CURRENT_TIMESTAMP);
        funMap.put("CURDATE", SqlStdOperatorTable.CURRENT_DATE);
        funMap.put("CURTIME", SqlStdOperatorTable.CURRENT_TIME);

        // number
        registerFunction(
            FormatFun.NAME,
            ReturnTypes.VARCHAR_2000,
            DingoInferTypes.DECIMAL,
            OperandTypes.NUMERIC_NUMERIC,
            SqlFunctionCategory.STRING
        );
        registerFunction(
            PowFun.NAME,
            ReturnTypes.explicit(SqlTypeName.DECIMAL),
            DingoInferTypes.DOUBLE,
            OperandTypes.NUMERIC_NUMERIC,
            SqlFunctionCategory.STRING
        );

        // string
        registerFunction(
            ConcatFun.NAME,
            ReturnTypes.VARCHAR_2000,
            InferTypes.VARCHAR_1024,
            OperandTypes.STRING_STRING,
            SqlFunctionCategory.STRING
        );
        registerFunction(
            LeftFun.NAME,
            ReturnTypes.VARCHAR_2000,
            DingoInferTypes.VARCHAR1024_INTEGER,
            OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.NUMERIC),
            SqlFunctionCategory.STRING
        );
        registerFunction(
            LocateFun.NAME,
            ReturnTypes.INTEGER,
            InferTypes.VARCHAR_1024,
            OperandTypes.STRING_STRING,
            SqlFunctionCategory.NUMERIC
        );
        registerFunction(
            LTrimFun.NAME,
            ReturnTypes.VARCHAR_2000,
            InferTypes.VARCHAR_1024,
            OperandTypes.STRING,
            SqlFunctionCategory.STRING
        );
        registerFunction(
            MidFun.NAME,
            ReturnTypes.VARCHAR_2000,
            DingoInferTypes.VARCHAR1024_INTEGER_INTEGER,
            OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
            SqlFunctionCategory.STRING
        );
        registerFunction(
            MidFun.NAME,
            ReturnTypes.VARCHAR_2000,
            DingoInferTypes.VARCHAR1024_INTEGER,
            OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.NUMERIC),
            SqlFunctionCategory.STRING,
            1
        );
        registerFunction(
            RepeatFun.NAME,
            ReturnTypes.VARCHAR_2000,
            DingoInferTypes.VARCHAR1024_INTEGER,
            OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.NUMERIC),
            SqlFunctionCategory.STRING
        );
        registerFunction(
            ReverseFun.NAME,
            ReturnTypes.VARCHAR_2000,
            InferTypes.VARCHAR_1024,
            OperandTypes.STRING,
            SqlFunctionCategory.STRING
        );
        registerFunction(
            RightFun.NAME,
            ReturnTypes.VARCHAR_2000,
            DingoInferTypes.VARCHAR1024_INTEGER,
            OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.NUMERIC),
            SqlFunctionCategory.STRING
        );
        registerFunction(
            RTrimFun.NAME,
            ReturnTypes.VARCHAR_2000,
            InferTypes.VARCHAR_1024,
            OperandTypes.STRING,
            SqlFunctionCategory.STRING
        );

        // time
        registerFunction(
            FromUnixTimeFun.NAME,
            ReturnTypes.TIMESTAMP,
            InferTypes.VARCHAR_1024,
            OperandTypes.NUMERIC,
            SqlFunctionCategory.STRING
        );
        registerFunction(
            UnixTimestampEvaluators.NAME,
            ReturnTypes.BIGINT,
            DingoInferTypes.TIMESTAMP,
            OperandTypes.or(
                OperandTypes.NILADIC,
                OperandTypes.TIMESTAMP,
                OperandTypes.NUMERIC
            ),
            SqlFunctionCategory.NUMERIC
        );
        registerFunction(
            DateFormatFun.NAME,
            ReturnTypes.VARCHAR_2000,
            DingoInferTypes.DATE_VARCHAR1024,
            OperandTypes.family(SqlTypeFamily.DATE, SqlTypeFamily.STRING),
            SqlFunctionCategory.STRING
        );
        registerFunction(
            TimeFormatFun.NAME,
            ReturnTypes.VARCHAR_2000,
            DingoInferTypes.TIME_VARCHAR1024,
            OperandTypes.family(SqlTypeFamily.TIME, SqlTypeFamily.STRING),
            SqlFunctionCategory.STRING
        );
        registerFunction(
            TimestampFormatFun.NAME,
            ReturnTypes.VARCHAR_2000,
            DingoInferTypes.TIMESTAMP_VARCHAR1024,
            OperandTypes.family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.STRING),
            SqlFunctionCategory.STRING
        );
        registerFunction(
            DateDiffFun.NAME,
            ReturnTypes.BIGINT,
            DingoInferTypes.DATE_DATE,
            OperandTypes.family(SqlTypeFamily.DATE, SqlTypeFamily.DATE),
            SqlFunctionCategory.NUMERIC
        );
    }

    public void registerFunction(
        @NonNull String name,
        @Nullable SqlReturnTypeInference returnTypeInference,
        @Nullable SqlOperandTypeInference operandTypeInference,
        @Nullable SqlOperandTypeChecker operandTypeChecker,
        SqlFunctionCategory category
    ) {
        registerFunction(name, returnTypeInference, operandTypeInference, operandTypeChecker, category, 0);
    }

    public void registerFunction(
        @NonNull String name,
        @Nullable SqlReturnTypeInference returnTypeInference,
        @Nullable SqlOperandTypeInference operandTypeInference,
        @Nullable SqlOperandTypeChecker operandTypeChecker,
        SqlFunctionCategory category,
        int id
    ) {
        funMap.put(name.toUpperCase(), new DingoSqlFunction(
            name.toUpperCase(),
            returnTypeInference,
            operandTypeInference,
            operandTypeChecker,
            category,
            id
        ));
    }

    @Override
    public void lookupOperatorOverloads(
        SqlIdentifier opName,
        @Nullable SqlFunctionCategory category,
        SqlSyntax syntax,
        List<SqlOperator> operatorList,
        SqlNameMatcher nameMatcher
    ) {
        if (syntax != SqlSyntax.FUNCTION) {
            return;
        }
        Collection<SqlFunction> functions = funMap.get(opName.getSimple().toUpperCase());
        operatorList.addAll(functions);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Only used for debug.
     */
    @Override
    public List<SqlOperator> getOperatorList() {
        return new ArrayList<>(funMap.values());
    }
}
