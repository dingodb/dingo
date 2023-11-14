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
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

@JsonPropertyOrder({"name", "type", "precision", "scale", "nullable", "primary", "default",})
@EqualsAndHashCode
@Builder
@Slf4j
public class ColumnDefinition {
    public static final int DEFAULT_PRECISION = -1;
    public static final int DEFAULT_SCALE = Integer.MIN_VALUE;

    public static final int NORMAL_STATE = 1;
    public static final int HIDE_STATE = 2;
    public static final int DELETE_STATE = -1;

    @JsonProperty(value = "name", required = true)
    @Getter
    private final String name;

    @JsonProperty(value = "sqlType", required = true)
    private final String type;

    // Element type of ARRAY & MULTISET
    @JsonProperty(value = "elementType")
    @Getter
    private final String elementType;

    @JsonProperty(value = "precision")
    @JsonSerialize(using = PrecisionSerializer.class)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Getter
    @Builder.Default
    private final int precision = DEFAULT_PRECISION;

    @JsonProperty("scale")
    @JsonSerialize(using = ScaleSerializer.class)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Getter
    @Builder.Default
    private final int scale = DEFAULT_SCALE;

    @SuppressWarnings("FieldMayBeStatic")
    @JsonProperty("nullable")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @Builder.Default
    private final boolean nullable = true;

    @SuppressWarnings("FieldMayBeStatic")
    @JsonProperty("primary")
    @Getter
    @Builder.Default
    private final int primary = -1;

    @SuppressWarnings("FieldMayBeStatic")
    @JsonProperty("default")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    @Getter
    @Builder.Default
    private final String defaultValue = null;

    @JsonProperty("autoIncrement")
    @Getter
    @Builder.Default
    private final boolean autoIncrement = false;

    @JsonProperty("state")
    @Getter
    @Setter
    @Builder.Default
    private int state = NORMAL_STATE;

    @JsonProperty("comment")
    @Getter
    @Builder.Default
    private final String comment = "";

    @JsonCreator
    public static ColumnDefinition getInstance(
        @JsonProperty("name") String name,
        @JsonProperty("type") @NonNull String type,
        @JsonProperty("elementType") String elementTypeName,
        @JsonProperty("precision") Integer precision,
        @JsonProperty("scale") Integer scale,
        @JsonProperty("nullable") boolean nullable,
        @JsonProperty("primary") int primary,
        @JsonProperty("default") String defaultValue,
        @JsonProperty("autoIncrement") boolean autoIncrement,
        @JsonProperty("state") int state
    ) {
        return builder()
            .name(name)
            .type(type)
            .elementType(elementTypeName)
            .precision(precision == null ? DEFAULT_PRECISION : precision)
            .scale(scale == null ? DEFAULT_SCALE : scale)
            .nullable(nullable)
            .primary(primary)
            .defaultValue(defaultValue)
            .autoIncrement(autoIncrement)
            .state(state)
            .build();
    }

    public boolean isPrimary() {
        return primary > -1;
    }

    public DingoType getType() {
        return DingoTypeFactory.INSTANCE.fromName(type, elementType, nullable);
    }

    public String getTypeName() {
        return type;
    }

    public boolean isNullable() {
        return nullable;
    }
}
