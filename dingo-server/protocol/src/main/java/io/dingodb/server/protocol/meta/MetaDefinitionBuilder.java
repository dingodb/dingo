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

package io.dingodb.server.protocol.meta;

import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;

import java.util.ArrayList;
import java.util.List;

public class MetaDefinitionBuilder {

    private String name;
    private List<String> cols = new ArrayList<>();

    public MetaDefinitionBuilder(String name) {
        this.name = name;
    }

    public MetaDefinitionBuilder addColumn(String name) {
        cols.add(name);
        return this;
    }

    public MetaDefinitionBuilder addColumns(List<String> names) {
        cols.addAll(names);
        return this;
    }

    private ColumnDefinition buildColumn(String name) {
        return ColumnDefinition.builder()
            .name(name)
            .type("VARCHAR")
            .build();
    }

    public TableDefinition build() {
        synchronized (System.getProperties()) {
            TableDefinition definition = new TableDefinition(name)
                .addColumn(ColumnDefinition.builder()
                    .name("id")
                    .type("VARCHAR")
                    .nullable(false)
                    .primary(true)
                    .build());
            cols.forEach(col -> definition.addColumn(buildColumn(col)));
            return definition;
        }
    }

}
