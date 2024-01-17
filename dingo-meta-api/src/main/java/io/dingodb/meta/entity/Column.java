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
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.NullableType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

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

    public boolean isNullable() {
        return type instanceof NullableType && ((NullableType) type).isNullable();
    }

    public boolean isPrimary() {
        return primaryKeyIndex >= 0;
    }

}
