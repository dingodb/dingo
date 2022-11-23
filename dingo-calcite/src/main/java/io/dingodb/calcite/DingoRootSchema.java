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

import io.dingodb.meta.MetaService;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Collections;
import java.util.Map;

public class DingoRootSchema extends AbstractSchema {
    public static final String ROOT_SCHEMA_NAME = "DINGO_ROOT";
    //public static final String DEFAULT_SCHEMA_NAME = ROOT_SCHEMA_NAME;
    public static final String DEFAULT_SCHEMA_NAME = "DINGO";
    public static final DingoRootSchema ROOT = new DingoRootSchema();

    private static final MetaService ROOT_META_SERVICE = MetaService.root();

    private DingoRootSchema() {
    }

    @Override
    protected Map<String, Schema> getSubSchemaMap() {
        return Collections.singletonMap(DEFAULT_SCHEMA_NAME, new DingoSchema(MetaService.root()));
        // todo meta cache support multi schema
        //return ROOT_META_SERVICE.getSubMetaServices().entrySet().stream()
        //    .collect(Collectors.toMap(Map.Entry::getKey, e -> new DingoSchema(e.getValue())));
    }
}
