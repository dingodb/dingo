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

package io.dingodb.meta.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.ListType;
import io.dingodb.common.type.MapType;
import io.dingodb.common.type.NullableType;
import io.dingodb.common.type.scalar.BooleanType;
import io.dingodb.common.type.scalar.DateType;
import io.dingodb.common.type.scalar.DecimalType;
import io.dingodb.common.type.scalar.DoubleType;
import io.dingodb.common.type.scalar.FloatType;
import io.dingodb.common.type.scalar.IntegerType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.type.scalar.StringType;
import io.dingodb.common.type.scalar.TimeType;
import io.dingodb.common.type.scalar.TimestampType;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

@Getter
@Builder
@ToString
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Column {

    @JsonProperty
    @EqualsAndHashCode.Include
    public final String name;

    @JsonProperty
    @EqualsAndHashCode.Include
    public final DingoType type;

    @JsonProperty
    public final String sqlTypeName;
    @JsonProperty
    public final String elementTypeName;

    @JsonProperty
    @Builder.Default
    public final int precision = ColumnDefinition.DEFAULT_PRECISION;
    @JsonProperty
    @Builder.Default
    public final int scale = ColumnDefinition.DEFAULT_SCALE;

    @JsonProperty
    public final int primaryKeyIndex;

    @JsonProperty
    public final boolean autoIncrement;

    @JsonProperty
    public final int state;

    @JsonProperty
    public final String comment;
    @JsonProperty
    public final String defaultValueExpr;

    @JsonProperty
    public SchemaState schemaState;

    public boolean isNullable() {
        return type instanceof NullableType && ((NullableType) type).isNullable();
    }

    public boolean isPrimary() {
        return primaryKeyIndex >= 0;
    }

    public Column copy() {
        return Column.builder()
            .name(name)
            .primaryKeyIndex(primaryKeyIndex)
            .type(type)
            .scale(scale)
            .precision(precision)
            .state(state)
            .autoIncrement(autoIncrement)
            .defaultValueExpr(defaultValueExpr)
            .sqlTypeName(sqlTypeName)
            .elementTypeName(elementTypeName)
            .comment(comment)
            .schemaState(schemaState)
            .build();
    }

    public Object getDefaultVal() {
        if (defaultValueExpr == null) {
            return null;
        }
        if (type instanceof StringType) {
            return defaultValueExpr;
        } else if (type instanceof LongType) {
            return Long.parseLong(defaultValueExpr);
        } else if (type instanceof IntegerType) {
            return Integer.parseInt(defaultValueExpr);
        } else if (type instanceof DoubleType) {
            return Double.parseDouble(defaultValueExpr);
        } else if (type instanceof FloatType) {
            return Float.parseFloat(defaultValueExpr);
        } else if (type instanceof DateType) {
            if ("current_date".equalsIgnoreCase(defaultValueExpr)) {
                return new Date(System.currentTimeMillis());
            }
            return DateTimeUtils.parseDate(defaultValueExpr);
        } else if (type instanceof DecimalType) {
            return new BigDecimal(defaultValueExpr);
        } else if (type instanceof BooleanType) {
            if (defaultValueExpr.equalsIgnoreCase("true")) {
                return 1;
            } else if (defaultValueExpr.equalsIgnoreCase("false")) {
                return 0;
            }
        } else if (type instanceof TimestampType) {
            if (defaultValueExpr.equalsIgnoreCase("current_timestamp")) {
                return new Timestamp(System.currentTimeMillis());
            }
            return DateTimeUtils.parseTimestamp(defaultValueExpr);
        } else if (type instanceof TimeType) {
            return DateTimeUtils.parseTime(defaultValueExpr);
        } else if (type instanceof ListType) {
            if ("{}".equalsIgnoreCase(defaultValueExpr)) {
                return new ArrayList<>();
            }
            List<String> list = Arrays.asList(defaultValueExpr.split(","));
            return list.stream().map(item -> {
                switch (elementTypeName) {
                    case "FLOAT":
                        return Float.parseFloat(item);
                    case "DOUBLE":
                        return Double.parseDouble(item);
                    case "INTEGER":
                        return Integer.parseInt(item);
                    case "LONG":
                        return Long.parseLong(item);
                    case "BOOLEAN":
                        return Boolean.parseBoolean(item);
                    case "DATE":
                        return DateTimeUtils.parseDate(item);
                    case "DECIMAL":
                        return new BigDecimal(item);
                    case "TIMESTAMP":
                        return DateTimeUtils.parseTimestamp(item);
                    case "TIME":
                        return DateTimeUtils.parseTime(item);
                    default:
                        return item;
                }
            }).collect(Collectors.toList());
        } else if (type instanceof MapType) {
            if ("{}".equalsIgnoreCase(defaultValueExpr)) {
                return new LinkedHashMap<>();
            }
            List<String> list = Arrays.asList(defaultValueExpr.split(","));
            LinkedHashMap<Object, Object> mapVal = new LinkedHashMap<>();
            for (int j = 0; j < list.size(); j += 2) {
                mapVal.put(list.get(j), list.get(j + 1));
            }
            return mapVal;
        }
        return defaultValueExpr;
    }

}
