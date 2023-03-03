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

package io.dingodb.driver.type;

import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.NullType;
import io.dingodb.common.type.TupleType;
import io.dingodb.expr.core.TypeCode;
import org.apache.calcite.avatica.ColumnMetaData;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Types;
import java.util.List;

import static io.dingodb.common.type.DingoTypeFactory.list;

public final class DingoTypeUtils {
    private DingoTypeUtils() {
    }

    private static DingoType fromAvaticaType(ColumnMetaData.AvaticaType avaticaType) {
        if (avaticaType instanceof ColumnMetaData.ScalarType) {
            return DingoTypeFactory.scalar(convertSqlTypeId(avaticaType.id), false);
        } else if (avaticaType instanceof ColumnMetaData.ArrayType) {
            ColumnMetaData.ArrayType arrayType = (ColumnMetaData.ArrayType) avaticaType;
            //return array(fromAvaticaType(arrayType.getComponent()), false);
            return list(fromAvaticaType(arrayType.getComponent()), false);
        } else if (avaticaType instanceof ColumnMetaData.StructType) {
            ColumnMetaData.StructType structType = (ColumnMetaData.StructType) avaticaType;
            return fromColumnMetaDataList(structType.columns);
        }
        throw new IllegalStateException("Unsupported avatica type \"" + avaticaType + "\".");
    }

    public static DingoType fromColumnMetaData(@NonNull ColumnMetaData colMeta) {
        switch (colMeta.type.id) {
            case Types.NULL:
                return NullType.NULL;
            case Types.ARRAY:
                ColumnMetaData.ArrayType arrayType = (ColumnMetaData.ArrayType) colMeta.type;
                //return array(fromAvaticaType(arrayType.getComponent()), colMeta.nullable != 0);
                return list(fromAvaticaType(arrayType.getComponent()), colMeta.nullable != 0);
            case Types.STRUCT:
                ColumnMetaData.StructType structType = (ColumnMetaData.StructType) colMeta.type;
                return fromColumnMetaDataList(structType.columns);
            default:
                return DingoTypeFactory.scalar(convertSqlTypeId(colMeta.type.id), colMeta.nullable != 0);
        }
    }

    public static @NonNull TupleType fromColumnMetaDataList(@NonNull List<ColumnMetaData> colMetaList) {
        return DingoTypeFactory.tuple(colMetaList.stream()
            .map(DingoTypeUtils::fromColumnMetaData)
            .toArray(DingoType[]::new));
    }

    private static int convertSqlTypeId(int typeId) {
        switch (typeId) {
            case Types.NULL:
                return TypeCode.NULL;
            case Types.INTEGER:
                return TypeCode.INT;
            case Types.BIGINT:
                return TypeCode.LONG;
            case Types.FLOAT:
                return TypeCode.FLOAT;
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
            case Types.BINARY:
                return TypeCode.BINARY;
            case Types.JAVA_OBJECT:
                return TypeCode.OBJECT;
            default:
                break;
        }
        throw new IllegalArgumentException("Unsupported sql type id \"" + typeId + "\".");
    }
}
