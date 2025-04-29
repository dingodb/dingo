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

import io.dingodb.common.log.LogUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Method;
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
                } else if (sourceType instanceof ArraySqlType && targetType instanceof ArraySqlType) {
                    ArraySqlType type0 = (ArraySqlType) sourceType;
                    ArraySqlType type1 = (ArraySqlType) targetType;
                    if (type0.getComponentType().getSqlTypeName().getName().equalsIgnoreCase("BOOLEAN")
                        && type1.getComponentType().getSqlTypeName().getName().equalsIgnoreCase("INTEGER")) {
                        continue;
                    } else if (type1.getComponentType().getSqlTypeName().getName().equalsIgnoreCase("BOOLEAN")
                        && type0.getComponentType().getSqlTypeName().getName().equalsIgnoreCase("INTEGER")) {
                        continue;
                    }
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
}
