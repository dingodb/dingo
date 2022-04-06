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

package io.dingodb.common.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.jackson.PrecisionSerializer;
import io.dingodb.common.jackson.ScaleSerializer;
import io.dingodb.common.jackson.SqlTypeNameSerializer;
import io.dingodb.common.util.TypeMapping;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import javax.annotation.Nonnull;

@JsonPropertyOrder({"name", "type", "precision", "scale", "nullable", "primary", "default"})
@EqualsAndHashCode
@Builder
public class ColumnDefinition {
    @JsonProperty(value = "name", required = true)
    @Getter
    private final String name;

    @JsonProperty(value = "type", required = true)
    @JsonSerialize(using = SqlTypeNameSerializer.class)
    @Getter
    private final SqlTypeName type;

    @JsonProperty(value = "precision")
    @JsonSerialize(using = PrecisionSerializer.class)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Getter
    @Builder.Default
    private final int precision = RelDataType.PRECISION_NOT_SPECIFIED;

    @JsonProperty("scale")
    @JsonSerialize(using = ScaleSerializer.class)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Getter
    @Builder.Default
    private final int scale = RelDataType.SCALE_NOT_SPECIFIED;

    @SuppressWarnings("FieldMayBeStatic")
    @JsonProperty("notNull")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @Getter
    @Builder.Default
    private final boolean notNull = false;

    @SuppressWarnings("FieldMayBeStatic")
    @JsonProperty("primary")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @Getter
    @Builder.Default
    private final boolean primary = false;

    @JsonProperty("default")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @Getter
    @Builder.Default
    private final Object defaultValue = null;

    @JsonCreator
    public static ColumnDefinition getInstance(
        @JsonProperty("name") String name,
        @Nonnull @JsonProperty("type") String typeName,
        @JsonProperty("precision") Integer precision,
        @JsonProperty("scale") Integer scale,
        @JsonProperty("notNull") boolean notNull,
        @JsonProperty("primary") boolean primary,
        @JsonProperty("default") Object defaultValue
    ) {
        SqlTypeName type = SqlTypeName.get(typeName.toUpperCase());
        /*
        if (defaultValue != null) {
            notNull = true;
        }
         */
        if (type != null) {
            return builder()
                .name(name)
                .type(type)
                .precision(precision != null ? precision : RelDataType.PRECISION_NOT_SPECIFIED)
                .scale(scale != null ? scale : RelDataType.SCALE_NOT_SPECIFIED)
                .notNull(notNull)
                .primary(primary)
                .defaultValue(defaultValue)
                .build();
        }
        throw new AssertionError("Invalid type name \"" + typeName + "\".");
    }

    public static ColumnDefinition fromRelDataType(String name, @Nonnull RelDataType relDataType) {
        return ColumnDefinition.builder()
            .name(name)
            .type(relDataType.getSqlTypeName())
            .precision(relDataType.getPrecision())
            .scale(relDataType.getScale())
            .notNull(!relDataType.isNullable())
            .build();
    }

    public RelDataType getRelDataType(@NonNull RelDataTypeFactory typeFactory) {
        RelDataType result = typeFactory.createSqlType(type);
        if (precision != RelDataType.PRECISION_NOT_SPECIFIED) {
            if (scale != RelDataType.SCALE_NOT_SPECIFIED) {
                result = typeFactory.createSqlType(type, precision, scale);
            } else {
                result = typeFactory.createSqlType(type, precision);
            }
        }
        return typeFactory.createTypeWithNullability(result, !this.notNull);
    }

    public int getTypeCode() {
        return TypeMapping.formSqlTypeName(type);
    }

    public ElementSchema getElementType() {
        return new ElementSchema(getTypeCode());
    }
}
