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

package io.dingodb.calcite.type.converter;

import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.NullType;
import io.dingodb.common.type.TupleType;
import io.dingodb.common.util.Optional;
import io.dingodb.expr.core.TypeCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Types;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.dingodb.common.type.DingoTypeFactory.fromName;
import static io.dingodb.common.type.DingoTypeFactory.list;
import static io.dingodb.common.type.DingoTypeFactory.map;
import static io.dingodb.common.type.DingoTypeFactory.scalar;
import static io.dingodb.expr.core.TypeCode.BINARY;
import static io.dingodb.expr.core.TypeCode.BOOL;
import static io.dingodb.expr.core.TypeCode.DATE;
import static io.dingodb.expr.core.TypeCode.DECIMAL;
import static io.dingodb.expr.core.TypeCode.DOUBLE;
import static io.dingodb.expr.core.TypeCode.FLOAT;
import static io.dingodb.expr.core.TypeCode.INT;
import static io.dingodb.expr.core.TypeCode.LONG;
import static io.dingodb.expr.core.TypeCode.STRING;
import static io.dingodb.expr.core.TypeCode.TIME;
import static io.dingodb.expr.core.TypeCode.TIMESTAMP;

@Slf4j
public final class DefinitionMapper {

    private DefinitionMapper() {
    }

    public static SqlTypeName mapToSqlTypeName(DingoType type) {
        return SqlTypeName.get(TypeCode.nameOf(type.getTypeCode()));
    }

    public static DingoType mapToDingoType(SqlTypeName columnType, SqlTypeName elementType, boolean nullable) {
        return fromName(columnType.getName(), Optional.mapOrNull(elementType, SqlTypeName::getName), nullable);
    }

    public static DingoType mapToDingoType(TableDefinition table) {
        return DingoTypeFactory.tuple(
            table.getColumns().stream().map(ColumnDefinition::getType).toArray(DingoType[]::new)
        );
    }

    public static DingoType mapToDingoType(@NonNull ColumnMetaData colMeta) {
        switch (colMeta.type.id) {
            case Types.NULL:
                return NullType.NULL;
            case Types.ARRAY:
                ColumnMetaData.ArrayType arrayType = (ColumnMetaData.ArrayType) colMeta.type;
                //return array(fromAvaticaType(arrayType.getComponent()), colMeta.nullable != 0);
                return list(mapToDingoType(arrayType.getComponent()), colMeta.nullable != 0);
            case Types.STRUCT:
                ColumnMetaData.StructType structType = (ColumnMetaData.StructType) colMeta.type;
                return mapToDingoType(structType.columns);
            default:
                return scalar(sqlTypeIdToDingoTypeCode(colMeta.type.id), colMeta.nullable != 0);
        }
    }

    public static @NonNull TupleType mapToDingoType(@NonNull List<ColumnMetaData> colMetaList) {
        return DingoTypeFactory.tuple(colMetaList.stream()
            .map(DefinitionMapper::mapToDingoType)
            .toArray(DingoType[]::new));
    }

    public static DingoType mapToDingoType(ColumnMetaData.AvaticaType avaticaType) {
        if (avaticaType instanceof ColumnMetaData.ScalarType) {
            return scalar(sqlTypeIdToDingoTypeCode(avaticaType.id), false);
        } else if (avaticaType instanceof ColumnMetaData.ArrayType) {
            ColumnMetaData.ArrayType arrayType = (ColumnMetaData.ArrayType) avaticaType;
            //return array(fromAvaticaType(arrayType.getComponent()), false);
            return list(mapToDingoType(arrayType.getComponent()), false);
        } else if (avaticaType instanceof ColumnMetaData.StructType) {
            ColumnMetaData.StructType structType = (ColumnMetaData.StructType) avaticaType;
            return mapToDingoType(structType.columns);
        }
        throw new IllegalStateException("Unsupported avatica type \"" + avaticaType + "\".");
    }

    public static @NonNull DingoType mapToDingoType(@NonNull RelDataType relDataType) {
        if (!relDataType.isStruct()) {
            SqlTypeName sqlTypeName = relDataType.getSqlTypeName();
            switch (sqlTypeName) {
                case NULL:
                    return NullType.NULL;
                case ARRAY:
                case MULTISET: // MultiSet is implemented by list.
                    DingoType elementType = mapToDingoType(Objects.requireNonNull(relDataType.getComponentType()));
                    //return array(elementType, relDataType.isNullable());
                    return list(elementType, relDataType.isNullable());
                case MAP:
                    DingoType keyType = mapToDingoType(Objects.requireNonNull(relDataType.getKeyType()));
                    DingoType valueType = mapToDingoType(Objects.requireNonNull(relDataType.getValueType()));
                    return map(keyType, valueType, relDataType.isNullable());
                default:
                    return scalar(
                        TypeCode.codeOf(relDataType.getSqlTypeName().getName()),
                        relDataType.isNullable()
                    );
            }
        } else {
            return DingoTypeFactory.tuple(
                relDataType.getFieldList().stream()
                    .map(RelDataTypeField::getType)
                    .map(DefinitionMapper::mapToDingoType)
                    .toArray(DingoType[]::new)
            );
        }
    }

    public static RelDataType mapToRelDataType(
        @NonNull ColumnDefinition column, @NonNull RelDataTypeFactory typeFactory
    ) {
        RelDataType relDataType;
        SqlTypeName type = SqlTypeName.get(column.getTypeName().toUpperCase());
        switch (type) {
            case ARRAY:
                relDataType = typeFactory.createArrayType(
                    typeFactory.createSqlType(SqlTypeName.get(column.getElementType().toUpperCase())), -1
                );
                break;
            case MULTISET:
                relDataType = typeFactory.createMultisetType(
                    typeFactory.createSqlType(SqlTypeName.get(column.getElementType().toUpperCase())), -1
                );
                break;
            case MAP:
                relDataType = typeFactory.createMapType(
                    typeFactory.createSqlType(SqlTypeName.VARCHAR),
                    typeFactory.createSqlType(SqlTypeName.INTEGER)
                );
                break;
            default:
                if (column.getPrecision() != RelDataType.PRECISION_NOT_SPECIFIED) {
                    if (column.getScale() != RelDataType.SCALE_NOT_SPECIFIED) {
                        relDataType = typeFactory.createSqlType(type, column.getPrecision(), column.getScale());
                    } else {
                        relDataType = typeFactory.createSqlType(type, column.getPrecision());
                    }
                } else {
                    relDataType = typeFactory.createSqlType(type);
                }
        }
        return typeFactory.createTypeWithNullability(relDataType, column.isNullable());
    }

    public static RelDataType mapToRelDataType(TableDefinition table, @NonNull RelDataTypeFactory typeFactory) {
        // make column name uppercase to adapt to calcite
        List<ColumnDefinition> columns = table.getColumns();
        return typeFactory.createStructType(
            columns.stream().map(c -> mapToRelDataType(c, typeFactory)).collect(Collectors.toList()),
            columns.stream().map(ColumnDefinition::getName).map(String::toUpperCase).collect(Collectors.toList())
        );
    }

    private static int sqlTypeIdToDingoTypeCode(int typeId) {
        switch (typeId) {
            case Types.INTEGER:
                return INT;
            case Types.BIGINT:
                return LONG;
            case Types.FLOAT:
                return FLOAT;
            case Types.DOUBLE:
            case Types.REAL:
                return DOUBLE;
            case Types.BOOLEAN:
                return BOOL;
            case Types.DECIMAL:
                return DECIMAL;
            case Types.CHAR:
            case Types.VARCHAR:
                return STRING;
            case Types.DATE:
                return DATE;
            case Types.TIME:
                return TIME;
            case Types.TIMESTAMP:
                return TIMESTAMP;
            case Types.BINARY:
                return BINARY;
            case Types.JAVA_OBJECT:
                return TypeCode.OBJECT;
            default:
                break;
        }
        throw new IllegalArgumentException("Unsupported sql type id \"" + typeId + "\".");
    }

}
