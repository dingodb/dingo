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

package org.apache.calcite.sql.validate.implicit;

import com.google.common.collect.ImmutableList;
import io.dingodb.common.log.LogUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.type.DingoSqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

@Slf4j
public class DingoTypeCoercionImpl extends TypeCoercionImpl {
    public DingoTypeCoercionImpl(RelDataTypeFactory typeFactory, SqlValidator validator) {
        super(typeFactory, validator);
    }

    @Override public boolean querySourceCoercion(@Nullable SqlValidatorScope scope,
                                                 RelDataType sourceRowType, RelDataType targetRowType, SqlNode query) {
        final List<RelDataTypeField> sourceFields = sourceRowType.getFieldList();
        final List<RelDataTypeField> targetFields = targetRowType.getFieldList();
        final int sourceCount = sourceFields.size();
        for (int i = 0; i < sourceCount; i++) {
            RelDataType sourceType = sourceFields.get(i).getType();
            RelDataType targetType = targetFields.get(i).getType();
            if (!SqlTypeUtil.equalSansNullability(validator.getTypeFactory(), sourceType, targetType)
                && !SqlTypeUtil.canCastFrom(targetType, sourceType, true)) {
                // Returns early if types not equals and can not do type coercion.
                if (targetType.getSqlTypeName().getName().equalsIgnoreCase("DECIMAL")
                    && sourceType.getSqlTypeName().getName().equalsIgnoreCase("BINARY")) {
                    continue;
                } else if (targetType.getSqlTypeName().getName().equalsIgnoreCase("BOOLEAN")
                    && sourceType.getSqlTypeName().getName().equalsIgnoreCase("BINARY")) {
                    continue;
                } else if (targetType.getSqlTypeName().getName().equalsIgnoreCase("FLOAT")
                    && sourceType.getSqlTypeName().getName().equalsIgnoreCase("BINARY")) {
                    continue;
                } else if (targetType.getSqlTypeName().getName().equalsIgnoreCase("DOUBLE")
                    && sourceType.getSqlTypeName().getName().equalsIgnoreCase("BINARY")) {
                    continue;
                } else {
                    return false;
                }
            }
        }
        boolean coerced = false;
        for (int i = 0; i < sourceFields.size(); i++) {
            RelDataType targetType = targetFields.get(i).getType();
            coerced = coerceSourceRowType1(scope, query, i, targetType) || coerced;
        }
        return coerced;
    }

    private boolean coerceSourceRowType1(
        @Nullable SqlValidatorScope sourceScope,
        SqlNode query,
        int columnIndex,
        RelDataType targetType) {
        switch (query.getKind()) {
            case INSERT:
                SqlInsert insert = (SqlInsert) query;
                return coerceSourceRowType1(sourceScope,
                    insert.getSource(),
                    columnIndex,
                    targetType);
            case UPDATE:
                SqlUpdate update = (SqlUpdate) query;
                final SqlNodeList sourceExpressionList = update.getSourceExpressionList();
                if (sourceExpressionList != null) {
                    return coerceColumnType(sourceScope, sourceExpressionList, columnIndex, targetType);
                } else {
                    // Note: this is dead code since sourceExpressionList is always non-null
                    return coerceSourceRowType1(sourceScope,
                        castNonNull(update.getSourceSelect()),
                        columnIndex,
                        targetType);
                }
            default:
                return rowTypeCoercion(sourceScope, query, columnIndex, targetType);
        }
    }

    public @Nullable RelDataType myImplicitCast(RelDataType in, DingoSqlTypeFamily expected) {
        List<DingoSqlTypeFamily> numericFamilies = ImmutableList.of(
            DingoSqlTypeFamily.NUMERIC,
            DingoSqlTypeFamily.DECIMAL,
            DingoSqlTypeFamily.APPROXIMATE_NUMERIC,
            DingoSqlTypeFamily.EXACT_NUMERIC,
            DingoSqlTypeFamily.INTEGER);
        List<DingoSqlTypeFamily> dateTimeFamilies = ImmutableList.of(DingoSqlTypeFamily.DATE,
            DingoSqlTypeFamily.TIME, DingoSqlTypeFamily.TIMESTAMP);
        // If the expected type is already a parent of the input type, no need to cast.
        if (expected.getTypeNames().contains(in.getSqlTypeName())) {
            return in;
        }
        // Cast null type (usually from null literals) into target type.
        if (SqlTypeUtil.isNull(in)) {
            return expected.getDefaultConcreteType(factory);
        }
        if (SqlTypeUtil.isNumeric(in) && expected == DingoSqlTypeFamily.DECIMAL) {
            return factory.decimalOf(in);
        }
        // FLOAT/DOUBLE -> DECIMAL
        if (SqlTypeUtil.isApproximateNumeric(in) && expected == DingoSqlTypeFamily.EXACT_NUMERIC) {
            return factory.decimalOf(in);
        }
        // DATE to TIMESTAMP
        if (SqlTypeUtil.isDate(in) && expected == DingoSqlTypeFamily.TIMESTAMP) {
            return factory.createSqlType(SqlTypeName.TIMESTAMP);
        }
        // TIMESTAMP to DATE.
        if (SqlTypeUtil.isTimestamp(in) && expected == DingoSqlTypeFamily.DATE) {
            return factory.createSqlType(SqlTypeName.DATE);
        }
        // If the function accepts any NUMERIC type and the input is a STRING,
        // returns the expected type family's default type.
        // REVIEW Danny 2018-05-22: same with MS-SQL and MYSQL.
        if (SqlTypeUtil.isCharacter(in) && numericFamilies.contains(expected)) {
            return expected.getDefaultConcreteType(factory);
        }
        // STRING + DATE -> DATE;
        // STRING + TIME -> TIME;
        // STRING + TIMESTAMP -> TIMESTAMP
        if (SqlTypeUtil.isCharacter(in) && dateTimeFamilies.contains(expected)) {
            return expected.getDefaultConcreteType(factory);
        }
        // STRING + BINARY -> VARBINARY
        if (SqlTypeUtil.isCharacter(in) && expected == DingoSqlTypeFamily.BINARY) {
            return expected.getDefaultConcreteType(factory);
        }
        // If we get here, `in` will never be STRING type.
        if (SqlTypeUtil.isAtomic(in)
            && (expected == DingoSqlTypeFamily.STRING
            || expected == DingoSqlTypeFamily.CHARACTER)) {
            return expected.getDefaultConcreteType(factory);
        }
        // CHAR -> GEOMETRY
        if (SqlTypeUtil.isCharacter(in) && expected == DingoSqlTypeFamily.GEO) {
            return expected.getDefaultConcreteType(factory);
        }
        return null;
    }

    private boolean myCanImplicitTypeCast(List<RelDataType> types, List<DingoSqlTypeFamily> families) {
        boolean needed = false;
        if (types.size() != families.size()) {
            return false;
        }
        for (Pair<RelDataType, DingoSqlTypeFamily> pair : Pair.zip(types, families)) {
            RelDataType implicitType = myImplicitCast(pair.left, pair.right);
            if (null == implicitType) {
                return false;
            }
            needed = pair.left != implicitType || needed;
        }
        return needed;
    }

    public static EnumMap<SqlTypeFamily, DingoSqlTypeFamily> faimily2DingoFaimily = new EnumMap<>(SqlTypeFamily.class);
    static {
        faimily2DingoFaimily.put(SqlTypeFamily.CHARACTER, DingoSqlTypeFamily.CHARACTER);
        faimily2DingoFaimily.put(SqlTypeFamily.BINARY, DingoSqlTypeFamily.BINARY);
        faimily2DingoFaimily.put(SqlTypeFamily.NUMERIC, DingoSqlTypeFamily.NUMERIC);
        faimily2DingoFaimily.put(SqlTypeFamily.DATE, DingoSqlTypeFamily.DATE);
        faimily2DingoFaimily.put(SqlTypeFamily.TIME, DingoSqlTypeFamily.TIME);
        faimily2DingoFaimily.put(SqlTypeFamily.TIMESTAMP, DingoSqlTypeFamily.TIMESTAMP);
        faimily2DingoFaimily.put(SqlTypeFamily.BOOLEAN, DingoSqlTypeFamily.BOOLEAN);
        faimily2DingoFaimily.put(SqlTypeFamily.INTERVAL_YEAR_MONTH, DingoSqlTypeFamily.INTERVAL_YEAR_MONTH);
        faimily2DingoFaimily.put(SqlTypeFamily.INTERVAL_DAY_TIME, DingoSqlTypeFamily.INTERVAL_DAY_TIME);
        faimily2DingoFaimily.put(SqlTypeFamily.STRING, DingoSqlTypeFamily.STRING);
        faimily2DingoFaimily.put(SqlTypeFamily.APPROXIMATE_NUMERIC, DingoSqlTypeFamily.APPROXIMATE_NUMERIC);
        faimily2DingoFaimily.put(SqlTypeFamily.EXACT_NUMERIC, DingoSqlTypeFamily.EXACT_NUMERIC);
        faimily2DingoFaimily.put(SqlTypeFamily.DECIMAL, DingoSqlTypeFamily.DECIMAL);
        faimily2DingoFaimily.put(SqlTypeFamily.INTEGER, DingoSqlTypeFamily.INTEGER);
        faimily2DingoFaimily.put(SqlTypeFamily.DATETIME, DingoSqlTypeFamily.DATETIME);
        faimily2DingoFaimily.put(SqlTypeFamily.DATETIME_INTERVAL, DingoSqlTypeFamily.DATETIME_INTERVAL);
        faimily2DingoFaimily.put(SqlTypeFamily.MULTISET, DingoSqlTypeFamily.MULTISET);
        faimily2DingoFaimily.put(SqlTypeFamily.ARRAY, DingoSqlTypeFamily.ARRAY);
        faimily2DingoFaimily.put(SqlTypeFamily.MAP, DingoSqlTypeFamily.MAP);
        faimily2DingoFaimily.put(SqlTypeFamily.NULL, DingoSqlTypeFamily.NULL);
        faimily2DingoFaimily.put(SqlTypeFamily.ANY, DingoSqlTypeFamily.ANY);
        faimily2DingoFaimily.put(SqlTypeFamily.CURSOR, DingoSqlTypeFamily.CURSOR);
        faimily2DingoFaimily.put(SqlTypeFamily.COLUMN_LIST, DingoSqlTypeFamily.COLUMN_LIST);
        faimily2DingoFaimily.put(SqlTypeFamily.GEO, DingoSqlTypeFamily.GEO);
        faimily2DingoFaimily.put(SqlTypeFamily.IGNORE, DingoSqlTypeFamily.IGNORE);
    }

    @Override
    public boolean builtinFunctionCoercion(
        SqlCallBinding binding,
        List<RelDataType> operandTypes,
        List<SqlTypeFamily> expectedFamilies) {

        List<DingoSqlTypeFamily> newExpectedFamilies = new ArrayList<>();
        for(SqlTypeFamily expectedFamily : expectedFamilies) {
            newExpectedFamilies.add(DingoTypeCoercionImpl.faimily2DingoFaimily.get(expectedFamily));
        }

        assert binding.getOperandCount() == operandTypes.size();
        if (!myCanImplicitTypeCast(operandTypes, newExpectedFamilies)) {
            return false;
        }
        boolean coerced = false;
        for (int i = 0; i < operandTypes.size(); i++) {
            RelDataType implicitType = myImplicitCast(operandTypes.get(i), newExpectedFamilies.get(i));
            coerced = null != implicitType
                && operandTypes.get(i) != implicitType
                && coerceOperandType(binding.getScope(), binding.getCall(), i, implicitType)
                || coerced;
        }
        return coerced;
    }
}
