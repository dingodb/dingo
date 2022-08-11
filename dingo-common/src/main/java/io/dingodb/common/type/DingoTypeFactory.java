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

import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.type.scalar.AbstractScalarType;
import io.dingodb.common.type.scalar.BinaryType;
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
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
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
            case TypeCode.BINARY:
                return new BinaryType(nullable);
            case TypeCode.OBJECT:
                return new ObjectType(nullable);
            default:
                break;
        }
        throw new IllegalArgumentException("Cannot create scalar type \"" + TypeCode.nameOf(typeCode) + "\".");
    }

    @Nonnull
    public static AbstractDingoType scalar(@Nonnull String typeString) {
        String[] v = typeString.split("\\|", 2);
        boolean nullable = v.length > 1 && v[1].equals(NullType.NULL.toString());
        return scalar(TypeCode.codeOf(v[0]), nullable);
    }

    @Nonnull
    public static TupleType tuple(DingoType[] fields) {
        return new TupleType(fields);
    }

    @Nonnull
    public static TupleType tuple(String... types) {
        return tuple(
            Arrays.stream(types)
                .map(DingoTypeFactory::scalar)
                .toArray(DingoType[]::new)
        );
    }

    @Nonnull
    public static ArrayType array(DingoType elementType, boolean nullable) {
        return new ArrayType(elementType, nullable);
    }

    @Nonnull
    public static ArrayType array(int elementTypeCode, boolean nullable) {
        return array(scalar(elementTypeCode, false), nullable);
    }

    @Nonnull
    public static ArrayType array(String type, boolean nullable) {
        return array(scalar(type), nullable);
    }

    @Nonnull
    public static ListType list(DingoType elementType, boolean nullable) {
        return new ListType(elementType, nullable);
    }

    @Nonnull
    public static ListType list(int elementTypeCode, boolean nullable) {
        return list(scalar(elementTypeCode, false), nullable);
    }

    @Nonnull
    public static ListType list(String type, boolean nullable) {
        return list(scalar(type), nullable);
    }

    @Nonnull
    public static MapType map(DingoType keyType, DingoType valueType, boolean nullable) {
        return new MapType(keyType, valueType, nullable);
    }

    @Nonnull
    public static MapType map(int keyTypeCode, int valueTypeCode, boolean nullable) {
        return map(scalar(keyTypeCode, false), scalar(valueTypeCode, false), nullable);
    }

    @Nonnull
    public static MapType map(String keyType, String valueType, boolean nullable) {
        return map(scalar(keyType), scalar(valueType), nullable);
    }

    @Nonnull
    public static DingoType fromColumnDefinition(@Nonnull ColumnDefinition columnDefinition) {
        SqlTypeName type = columnDefinition.getType();
        boolean notNull = columnDefinition.isNotNull();
        switch (type) {
            case ARRAY:
            case MULTISET:
                SqlTypeName elementType = columnDefinition.getElementType();
                //return DingoTypeFactory.array(TypeCode.codeOf(elementType.getName()), !notNull);
                return DingoTypeFactory.list(TypeCode.codeOf(elementType.getName()), !notNull);
            default:
                return DingoTypeFactory.scalar(TypeCode.codeOf(type.getName()), !notNull);
        }
    }

    @Nonnull
    public static DingoType fromRelDataType(@Nonnull RelDataType relDataType) {
        if (!relDataType.isStruct()) {
            SqlTypeName sqlTypeName = relDataType.getSqlTypeName();
            switch (sqlTypeName) {
                case NULL:
                    return NullType.NULL;
                case ARRAY:
                case MULTISET: // MultiSet is implemented by list.
                    DingoType elementType = fromRelDataType(Objects.requireNonNull(relDataType.getComponentType()));
                    //return array(elementType, relDataType.isNullable());
                    return list(elementType, relDataType.isNullable());
                case MAP:
                    DingoType keyType = fromRelDataType(Objects.requireNonNull(relDataType.getKeyType()));
                    DingoType valueType = fromRelDataType(Objects.requireNonNull(relDataType.getValueType()));
                    return map(keyType, valueType, relDataType.isNullable());
                default:
                    return scalar(
                        TypeCode.codeOf(relDataType.getSqlTypeName().getName()),
                        relDataType.isNullable()
                    );
            }
        } else {
            return tuple(
                relDataType.getFieldList().stream()
                    .map(RelDataTypeField::getType)
                    .map(DingoTypeFactory::fromRelDataType)
                    .toArray(DingoType[]::new)
            );
        }
    }

    @Nonnull
    private static DingoType fromAvaticaType(ColumnMetaData.AvaticaType avaticaType) {
        if (avaticaType instanceof ColumnMetaData.ScalarType) {
            return scalar(convertSqlTypeId(avaticaType.id), false);
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

    @Nonnull
    public static DingoType fromColumnMetaData(@Nonnull ColumnMetaData colMeta) {
        switch (colMeta.type.id) {
            case Types.ARRAY:
                ColumnMetaData.ArrayType arrayType = (ColumnMetaData.ArrayType) colMeta.type;
                //return array(fromAvaticaType(arrayType.getComponent()), colMeta.nullable != 0);
                return list(fromAvaticaType(arrayType.getComponent()), colMeta.nullable != 0);
            case Types.STRUCT:
                ColumnMetaData.StructType structType = (ColumnMetaData.StructType) colMeta.type;
                return fromColumnMetaDataList(structType.columns);
            default:
                return scalar(convertSqlTypeId(colMeta.type.id), colMeta.nullable != 0);
        }
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
