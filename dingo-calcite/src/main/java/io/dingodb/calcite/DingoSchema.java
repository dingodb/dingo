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

package io.dingodb.calcite;

import io.dingodb.common.table.TableDefinition;
import io.dingodb.ddl.MutableSchema;
import io.dingodb.meta.MetaService;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;

import java.util.LinkedHashMap;
import java.util.Map;

public class DingoSchema extends MutableSchema {
    DingoSchema(MetaService metaService) {
        super(metaService);
    }

    @Override
    protected Map<String, Schema> getSubSchemaMap() {
        return super.getSubSchemaMap();
    }

    @Override
    protected Map<String, Table> getTableMap() {
        Map<String, TableDefinition> tds = MetaCache.getTableDefinitionsMap();
        if (tds == null) {
            return super.getTableMap(); // empty map
        }
        Map<String, Table> tableMap = new LinkedHashMap<>();
        tds.forEach((name, td) -> tableMap.put(name, new DingoTable(td)));
        return tableMap;
    }
}
