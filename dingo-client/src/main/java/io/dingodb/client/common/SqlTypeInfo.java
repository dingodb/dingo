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

package io.dingodb.client.common;


import io.dingodb.client.annotation.DingoColumn;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class SqlTypeInfo {
    String sqlTypeName;
    String defaultValue;
    Integer scale;
    Integer precision;

    public SqlTypeInfo(String sqlTypeName) {
        this(sqlTypeName, null, null, null);
    }

    public SqlTypeInfo(String sqlTypeName, Integer precision, Integer scale, String defaultValue) {
        this.sqlTypeName = sqlTypeName;
        this.precision = precision;
        this.scale = scale;
        this.defaultValue = defaultValue;
    }

    public SqlTypeInfo(String sqlTypeName, DingoColumn dingoColumn) {
        this.sqlTypeName = sqlTypeName;

        Integer precision = dingoColumn.precision().trim().isEmpty() ? null : Integer.parseInt(dingoColumn.precision());
        this.precision = precision;

        Integer scale = dingoColumn.scale().trim().isEmpty() ? null : Integer.parseInt(dingoColumn.scale());
        this.scale = scale;

        String defaultValue = dingoColumn.defaultValue().trim().isEmpty() ?  null : dingoColumn.defaultValue();
        this.defaultValue = defaultValue;
    }
}
