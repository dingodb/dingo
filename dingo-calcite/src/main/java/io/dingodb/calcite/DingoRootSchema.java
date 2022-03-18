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

import io.dingodb.exec.Services;
import io.dingodb.meta.MetaService;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.HashMap;
import java.util.Map;

public class DingoRootSchema extends AbstractSchema {
    public static final String ROOT_SCHEMA_NAME = "DINGO_ROOT";
    public static final String DEFAULT_SCHEMA_NAME = "DINGO";
    public static final DingoRootSchema ROOT = new DingoRootSchema();

    private DingoRootSchema() {
    }

    @Override
    protected Map<String, Schema> getSubSchemaMap() {
        Map<String, Schema> schemaMap = new HashMap<>(Services.metaServices.size());
        for (MetaService metaService : Services.metaServices.values()) {
            schemaMap.put(metaService.getName(), new DingoSchema(metaService));
        }
        return schemaMap;
    }
}
