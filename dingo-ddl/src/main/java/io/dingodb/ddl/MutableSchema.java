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

package io.dingodb.ddl;

import io.dingodb.common.table.TableDefinition;
import io.dingodb.meta.MetaService;
import lombok.Getter;
import org.apache.calcite.schema.impl.AbstractSchema;

import javax.annotation.Nonnull;

public abstract class MutableSchema extends AbstractSchema {
    @Getter
    protected final MetaService metaService;

    protected MutableSchema(MetaService metaService) {
        this.metaService = metaService;
    }

    public void createTable(@Nonnull String tableName, @Nonnull TableDefinition tableDefinition) {
        metaService.createTable(tableName, tableDefinition);
    }

    public boolean dropTable(@Nonnull String tableName) {
        return metaService.dropTable(tableName);
    }

    @Override
    public boolean isMutable() {
        return true;
    }
}
