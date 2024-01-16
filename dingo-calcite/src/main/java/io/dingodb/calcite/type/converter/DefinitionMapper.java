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
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
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

import static io.dingodb.common.type.DingoTypeFactory.list;
import static io.dingodb.common.type.DingoTypeFactory.map;

@Slf4j
public final class DefinitionMapper {
    private DefinitionMapper() {
    }

    public static DingoType mapToDingoType(SqlTypeName columnType, SqlTypeName elementType, boolean nullable) {
        return DingoTypeFactory.INSTANCE.fromName(
            columnType.getName(),
            Optional.mapOrNull(elementType, SqlTypeName::getName),
            nullable
        );
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
                return DingoTypeFactory.INSTANCE.scalar(colMeta.type.id, colMeta.nullable != 0);
        }
    }

    public static @NonNull TupleType mapToDingoType(@NonNull List<ColumnMetaData> colMetaList) {
        return DingoTypeFactory.tuple(colMetaList.stream()
            .map(DefinitionMapper::mapToDingoType)
            .toArray(DingoType[]::new));
    }

    public static DingoType mapToDingoType(ColumnMetaData.AvaticaType avaticaType) {
        if (avaticaType instanceof ColumnMetaData.ScalarType) {
            return DingoTypeFactory.INSTANCE.scalar(avaticaType.id, false);
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
                    return DingoTypeFactory.INSTANCE.scalar(
                        relDataType.getSqlTypeName().getName(),
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

        if (type == null) {
            return mapToJavaRelDataType(column.getTypeName().toUpperCase(), typeFactory);
        }

        switch (type) {
            case ARRAY:
                relDataType = typeFactory.createArrayType(
                    Optional.mapOrGet(
                        SqlTypeName.get(column.getElementType().toUpperCase()),
                        typeFactory::createSqlType,
                        () -> mapToJavaRelDataType(column.getElementType().toUpperCase(), typeFactory)
                    ),
                    -1
                );
                break;
            case MULTISET:
                relDataType = typeFactory.createMultisetType(
                    Optional.mapOrGet(
                        SqlTypeName.get(column.getElementType().toUpperCase()),
                        typeFactory::createSqlType,
                        () -> mapToJavaRelDataType(column.getElementType().toUpperCase(), typeFactory)
                    ),
                    -1
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

    public static RelDataType mapToJavaRelDataType(String typeName, RelDataTypeFactory typeFactory) {
        switch (typeName) {
            case "INT":
                return typeFactory.createJavaType(Integer.class);
            case "LONG":
                return typeFactory.createJavaType(Long.class);
            case "BOOL":
                return typeFactory.createJavaType(Boolean.class);
            case "STRING":
                return typeFactory.createJavaType(String.class);
        }
        return null;
    }

    public static RelDataType mapToRelDataType(TableDefinition table, @NonNull RelDataTypeFactory typeFactory) {
        // make column name uppercase to adapt to calcite
        List<ColumnDefinition> columns = table.getColumns();
        return typeFactory.createStructType(
            columns.stream().map(c -> mapToRelDataType(c, typeFactory)).collect(Collectors.toList()),
            columns.stream().map(ColumnDefinition::getName).map(String::toUpperCase).collect(Collectors.toList())
        );
    }

    public static RelDataType mapToRelDataType(Table table, @NonNull RelDataTypeFactory typeFactory) {
        // make column name uppercase to adapt to calcite
        List<Column> columns = table.getColumns();
        return typeFactory.createStructType(
            columns.stream().map(c -> mapToRelDataType(c, typeFactory)).collect(Collectors.toList()),
            columns.stream().map(Column::getName).map(String::toUpperCase).collect(Collectors.toList())
        );
    }

    public static RelDataType mapToRelDataType(
        @NonNull Column column, @NonNull RelDataTypeFactory typeFactory
    ) {
        RelDataType relDataType;
        SqlTypeName type = SqlTypeName.get(column.getSqlTypeName().toUpperCase());

        if (type == null) {
            return mapToJavaRelDataType(column.getSqlTypeName().toUpperCase(), typeFactory);
        }

        switch (type) {
            case ARRAY:
                relDataType = typeFactory.createArrayType(
                    Optional.mapOrGet(
                        SqlTypeName.get(column.getElementTypeName().toUpperCase()),
                        typeFactory::createSqlType,
                        () -> mapToJavaRelDataType(column.getElementTypeName().toUpperCase(), typeFactory)
                    ),
                    -1
                );
                break;
            case MULTISET:
                relDataType = typeFactory.createMultisetType(
                    Optional.mapOrGet(
                        SqlTypeName.get(column.getElementTypeName().toUpperCase()),
                        typeFactory::createSqlType,
                        () -> mapToJavaRelDataType(column.getElementTypeName().toUpperCase(), typeFactory)
                    ),
                    -1
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

}
