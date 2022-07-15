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

package io.dingodb.common.type;

import io.dingodb.common.type.scalar.AbstractScalarType;
import io.dingodb.common.type.scalar.BooleanType;
import io.dingodb.common.type.scalar.DateType;
import io.dingodb.common.type.scalar.DecimalType;
import io.dingodb.common.type.scalar.DoubleType;
import io.dingodb.common.type.scalar.IntegerType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.type.scalar.ObjectType;
import io.dingodb.common.type.scalar.StringType;
import io.dingodb.common.type.scalar.TimeType;
import io.dingodb.common.type.scalar.TimestampType;
import io.dingodb.expr.runtime.TypeCode;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;

public final class DingoTypeFactory {
    private DingoTypeFactory() {
    }

    @Nonnull
    public static AbstractScalarType scalar(int typeCode, boolean nullable) {
        switch (typeCode) {
            case TypeCode.INT:
                return new IntegerType(nullable);
            case TypeCode.STRING:
                return new StringType(nullable);
            case TypeCode.BOOL:
                return new BooleanType(nullable);
            case TypeCode.LONG:
                return new LongType(nullable);
            case TypeCode.DOUBLE:
                return new DoubleType(nullable);
            case TypeCode.DECIMAL:
                return new DecimalType(nullable);
            case TypeCode.DATE:
                return new DateType(nullable);
            case TypeCode.TIME:
                return new TimeType(nullable);
            case TypeCode.TIMESTAMP:
                return new TimestampType(nullable);
            case TypeCode.OBJECT:
                return new ObjectType(nullable);
            default:
                break;
        }
        throw new IllegalArgumentException("Cannot create scalar type \"" + TypeCode.nameOf(typeCode) + "\".");
    }

    @Nonnull
    public static TupleType tuple(String... types) {
        return new TupleType(
            Arrays.stream(types)
                .map(AbstractDingoType::scalar)
                .toArray(DingoType[]::new)
        );
    }

    @Nonnull
    public static TupleType tuple(DingoType[] types) {
        return new TupleType(types);
    }

    @Nonnull
    public static DingoType fromRelDataType(@Nonnull RelDataType relDataType) {
        if (!relDataType.isStruct()) {
            return scalar(
                TypeCode.codeOf(relDataType.getSqlTypeName().getName()),
                relDataType.isNullable()
            );
        } else {
            return new TupleType(
                relDataType.getFieldList().stream()
                    .map(RelDataTypeField::getType)
                    .map(DingoTypeFactory::fromRelDataType)
                    .toArray(DingoType[]::new)
            );
        }
    }

    @Nonnull
    public static DingoType fromColumnMetaData(@Nonnull ColumnMetaData colMeta) {
        return scalar(convertSqlTypeId(colMeta.type.id), colMeta.nullable != 0);
    }

    @Nonnull
    public static TupleType fromColumnMetaDataList(@Nonnull List<ColumnMetaData> colMetaList) {
        return tuple(colMetaList.stream()
            .map(DingoTypeFactory::fromColumnMetaData)
            .toArray(DingoType[]::new));
    }

    private static int convertSqlTypeId(int typeId) {
        switch (typeId) {
            case Types.INTEGER:
                return TypeCode.INT;
            case Types.BIGINT:
                return TypeCode.LONG;
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.REAL:
                return TypeCode.DOUBLE;
            case Types.BOOLEAN:
                return TypeCode.BOOL;
            case Types.DECIMAL:
                return TypeCode.DECIMAL;
            case Types.CHAR:
            case Types.VARCHAR:
                return TypeCode.STRING;
            case Types.DATE:
                return TypeCode.DATE;
            case Types.TIME:
                return TypeCode.TIME;
            case Types.TIMESTAMP:
                return TypeCode.TIMESTAMP;
            default:
                break;
        }
        throw new IllegalArgumentException("Unsupported sql type id \"" + typeId + "\".");
    }
}
