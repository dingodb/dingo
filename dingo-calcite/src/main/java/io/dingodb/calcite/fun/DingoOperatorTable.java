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
import io.dingodb.exec.fun.AutoIncrementFun;
import io.dingodb.exec.fun.PowFunFactory;
import io.dingodb.exec.fun.mysql.VersionFun;
import io.dingodb.exec.fun.special.ThrowFun;
import io.dingodb.exec.fun.vector.VectorCosineDistanceFun;
import io.dingodb.exec.fun.vector.VectorDistanceFun;
import io.dingodb.exec.fun.vector.VectorIPDistanceFun;
import io.dingodb.exec.fun.vector.VectorImageFun;
import io.dingodb.exec.fun.vector.VectorL2DistanceFun;
import io.dingodb.exec.fun.vector.VectorTextFun;
import io.dingodb.expr.runtime.op.string.ConcatFunFactory;
import io.dingodb.expr.runtime.op.string.LTrim1FunFactory;
import io.dingodb.expr.runtime.op.string.LeftFunFactory;
import io.dingodb.expr.runtime.op.string.Locate2FunFactory;
import io.dingodb.expr.runtime.op.string.Mid2FunFactory;
import io.dingodb.expr.runtime.op.string.NumberFormatFunFactory;
import io.dingodb.expr.runtime.op.string.RTrim1FunFactory;
import io.dingodb.expr.runtime.op.string.RepeatFunFactory;
import io.dingodb.expr.runtime.op.string.ReverseFunFactory;
import io.dingodb.expr.runtime.op.string.RightFunFactory;
import io.dingodb.expr.runtime.op.time.DateDiffFunFactory;
import io.dingodb.expr.runtime.op.time.DateFormat1FunFactory;
import io.dingodb.expr.runtime.op.time.FromUnixTimeFunFactory;
import io.dingodb.expr.runtime.op.time.TimeFormat1FunFactory;
import io.dingodb.expr.runtime.op.time.TimestampFormat1FunFactory;
import io.dingodb.expr.runtime.op.time.UnixTimestamp1FunFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
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

import static org.apache.calcite.sql.type.OperandTypes.family;
import static org.apache.calcite.sql.type.SqlAppointReturnTypeInference.FLOAT;
import static org.apache.calcite.sql.type.SqlAppointReturnTypeInference.ret;

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
            NumberFormatFunFactory.NAME,
            ReturnTypes.VARCHAR_2000_NULLABLE,
            DingoInferTypes.DECIMAL,
            OperandTypes.NUMERIC_NUMERIC,
            SqlFunctionCategory.STRING
        );
        registerFunction(
            PowFunFactory.NAME,
            (SqlOperatorBinding opBinding) -> {
                RelDataType type0 = opBinding.getOperandType(0);
                if (type0.getSqlTypeName().equals(SqlTypeName.FLOAT)
                    || type0.getSqlTypeName().equals(SqlTypeName.DOUBLE)
                ) {
                    return opBinding.getTypeFactory().createSqlType(SqlTypeName.DOUBLE);
                }
                return opBinding.getTypeFactory().createSqlType(SqlTypeName.DECIMAL);
            },
            DingoInferTypes.DOUBLE,
            OperandTypes.NUMERIC_NUMERIC,
            SqlFunctionCategory.STRING
        );

        // string
        registerFunction(
            ConcatFunFactory.NAME,
            ReturnTypes.VARCHAR_2000_NULLABLE,
            InferTypes.VARCHAR_1024,
            OperandTypes.STRING_STRING,
            SqlFunctionCategory.STRING
        );
        registerFunction(
            LeftFunFactory.NAME,
            ReturnTypes.VARCHAR_2000_NULLABLE,
            DingoInferTypes.VARCHAR1024_INTEGER,
            family(SqlTypeFamily.STRING, SqlTypeFamily.NUMERIC),
            SqlFunctionCategory.STRING
        );
        registerFunction(
            Locate2FunFactory.NAME,
            ReturnTypes.INTEGER_NULLABLE,
            InferTypes.VARCHAR_1024,
            OperandTypes.STRING_STRING,
            SqlFunctionCategory.NUMERIC
        );
        registerFunction(
            LTrim1FunFactory.NAME,
            ReturnTypes.VARCHAR_2000_NULLABLE,
            InferTypes.VARCHAR_1024,
            OperandTypes.STRING,
            SqlFunctionCategory.STRING
        );
        registerFunction(
            Mid2FunFactory.NAME,
            ReturnTypes.VARCHAR_2000_NULLABLE,
            DingoInferTypes.VARCHAR1024_INTEGER_INTEGER,
            OperandTypes.or(
                family(SqlTypeFamily.STRING, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
                family(SqlTypeFamily.STRING, SqlTypeFamily.NUMERIC)
            ),
            SqlFunctionCategory.STRING
        );
        registerFunction(
            RepeatFunFactory.NAME,
            ReturnTypes.VARCHAR_2000_NULLABLE,
            DingoInferTypes.VARCHAR1024_INTEGER,
            family(SqlTypeFamily.STRING, SqlTypeFamily.NUMERIC),
            SqlFunctionCategory.STRING
        );
        registerFunction(
            ReverseFunFactory.NAME,
            ReturnTypes.VARCHAR_2000_NULLABLE,
            InferTypes.VARCHAR_1024,
            OperandTypes.STRING,
            SqlFunctionCategory.STRING
        );
        registerFunction(
            RightFunFactory.NAME,
            ReturnTypes.VARCHAR_2000_NULLABLE,
            DingoInferTypes.VARCHAR1024_INTEGER,
            family(SqlTypeFamily.STRING, SqlTypeFamily.NUMERIC),
            SqlFunctionCategory.STRING
        );
        registerFunction(
            RTrim1FunFactory.NAME,
            ReturnTypes.VARCHAR_2000_NULLABLE,
            InferTypes.VARCHAR_1024,
            OperandTypes.STRING,
            SqlFunctionCategory.STRING
        );

        // time
        registerFunction(
            FromUnixTimeFunFactory.NAME,
            ReturnTypes.TIMESTAMP,
            DingoInferTypes.DECIMAL,
            OperandTypes.NUMERIC,
            SqlFunctionCategory.STRING
        );
        registerFunction(
            UnixTimestamp1FunFactory.NAME,
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
            DateFormat1FunFactory.NAME,
            ReturnTypes.VARCHAR_2000,
            DingoInferTypes.DATE_VARCHAR1024,
            // Why not `OperandTypes.family(ImmutableList.of(SqlTypeFamily.DATE, SqlTypeFamily.STRING), i -> i == 1)`?
            // Because if there are any optional operands, casting is disabled.
            OperandTypes.or(
                family(SqlTypeFamily.DATE, SqlTypeFamily.STRING),
                OperandTypes.DATE
            ),
            SqlFunctionCategory.STRING
        );
        registerFunction(
            TimeFormat1FunFactory.NAME,
            ReturnTypes.VARCHAR_2000,
            DingoInferTypes.TIME_VARCHAR1024,
            OperandTypes.or(
                family(SqlTypeFamily.TIME, SqlTypeFamily.STRING),
                family(SqlTypeFamily.TIME)
            ),
            SqlFunctionCategory.STRING
        );
        registerFunction(
            TimestampFormat1FunFactory.NAME,
            ReturnTypes.VARCHAR_2000,
            DingoInferTypes.TIMESTAMP_VARCHAR1024,
            OperandTypes.or(
                family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.STRING),
                OperandTypes.TIMESTAMP,
                family(SqlTypeFamily.NUMERIC, SqlTypeFamily.STRING),
                OperandTypes.NUMERIC
            ),
            SqlFunctionCategory.STRING
        );
        registerFunction(
            DateDiffFunFactory.NAME,
            ReturnTypes.BIGINT,
            DingoInferTypes.DATE_DATE,
            family(SqlTypeFamily.DATE, SqlTypeFamily.DATE),
            SqlFunctionCategory.NUMERIC
        );

        // special
        registerFunction(
            ThrowFun.NAME,
            ReturnTypes.VARCHAR_2000_NULLABLE,
            null,
            OperandTypes.NILADIC,
            SqlFunctionCategory.STRING
        );
        registerFunction(
            AutoIncrementFun.NAME,
            ReturnTypes.BIGINT,
            InferTypes.VARCHAR_1024,
            OperandTypes.STRING_STRING,
            SqlFunctionCategory.NUMERIC
        );
        registerFunction(
            VectorImageFun.NAME,
            ret,
            DingoInferTypes.VARCHAR1024_VARCHAR1024_BOOLEAN,
            family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.BOOLEAN),
            SqlFunctionCategory.NUMERIC
        );
        registerFunction(
            VectorTextFun.NAME,
            ret,
            DingoInferTypes.VARCHAR,
            OperandTypes.STRING_STRING,
            SqlFunctionCategory.NUMERIC
        );
        registerFunction(
            VectorL2DistanceFun.NAME,
            FLOAT,
            DingoInferTypes.FLOAT,
            family(SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY),
            SqlFunctionCategory.NUMERIC
        );
        registerFunction(
            VectorIPDistanceFun.NAME,
            FLOAT,
            DingoInferTypes.FLOAT,
            family(SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY),
            SqlFunctionCategory.NUMERIC
        );
        registerFunction(
            VectorCosineDistanceFun.NAME,
            FLOAT,
            DingoInferTypes.FLOAT,
            family(SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY),
            SqlFunctionCategory.NUMERIC
        );
        registerFunction(
            VectorDistanceFun.NAME,
            FLOAT,
            DingoInferTypes.FLOAT,
            family(SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY),
            SqlFunctionCategory.NUMERIC
        );
        registerFunction(
            VersionFun.NAME,
            ReturnTypes.VARCHAR_2000,
            null,
            OperandTypes.POSITIVE_INTEGER_LITERAL.or(OperandTypes.NILADIC),
            SqlFunctionCategory.STRING
        );
    }

    public void registerFunction(
        @NonNull String name,
        @Nullable SqlReturnTypeInference returnTypeInference,
        @Nullable SqlOperandTypeInference operandTypeInference,
        @Nullable SqlOperandTypeChecker operandTypeChecker,
        SqlFunctionCategory category
    ) {
        funMap.put(name.toUpperCase(), new DingoSqlFunction(
            name.toUpperCase(),
            returnTypeInference,
            operandTypeInference,
            operandTypeChecker,
            category
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
