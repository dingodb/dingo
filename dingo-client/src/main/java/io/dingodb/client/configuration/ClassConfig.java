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

package io.dingodb.client.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import javax.validation.constraints.NotNull;

@Getter
@Setter
public class ClassConfig {
    @JsonProperty(value = "class")
    private String className;
    private String database;
    private String table;

    private KeyConfig key;
    private String factoryClass;
    private String factoryMethod;
    private final List<ColumnConfig> columns;

    public ClassConfig() {
        columns = new ArrayList<>();
    }

    public ColumnConfig getColumnByName(@NotNull String name) {
        for (ColumnConfig thisBin : columns) {
            if (name.equals(thisBin.getName())) {
                return thisBin;
            }
        }
        return null;
    }

    public ColumnConfig getColumnByGetterName(@NotNull String getterName) {
        for (ColumnConfig thisBin : columns) {
            if (getterName.equals(thisBin.getGetter())) {
                return thisBin;
            }
        }
        return null;
    }

    public ColumnConfig getColumnByFieldName(@NotNull String fieldName) {
        for (ColumnConfig thisBin : columns) {
            if (fieldName.equals(thisBin.getField())) {
                return thisBin;
            }
        }
        return null;
    }

    public void validate() {
        for (ColumnConfig thisBin : columns) {
            thisBin.validate(this.className);
        }
    }
}
