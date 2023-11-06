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

package io.dingodb.calcite.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.dingodb.calcite.DingoParserContext;
import io.dingodb.common.CommonId;
import io.dingodb.common.util.Optional;
import io.dingodb.meta.MetaService;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;

import java.util.Set;

public class DingoRootSchema extends AbstractSchema {
    public static final String ROOT_SCHEMA_NAME = MetaService.ROOT_NAME;
    //public static final String DEFAULT_SCHEMA_NAME = ROOT_SCHEMA_NAME;
    public static final String DEFAULT_SCHEMA_NAME = MetaService.DINGO_NAME;

    private static final MetaService ROOT_META_SERVICE = MetaService.root();

    public DingoRootSchema(DingoParserContext context) {
        super(ROOT_META_SERVICE, context, ImmutableList.of(ROOT_SCHEMA_NAME));
    }

    @Override
    public Set<String> getTableNames() {
        return ImmutableSet.of();
    }

    @Override
    public Table getTable(String name) {
        return null;
    }

    @Override
    public CommonId getTableId(String tableName) {
        return null;
    }

    public void createSubSchema(String schemaName) {
        metaService.createSubMetaService(schemaName);
    }

    public void dropSubSchema(String schemaName) {
        metaService.dropSubMetaService(schemaName);
    }

    @Override
    public Schema getSubSchema(String name) {
        return Optional.mapOrNull(
            metaService.getSubMetaService(name),
            __ -> new DingoSchema(__, context, ImmutableList.of(ROOT_SCHEMA_NAME, __.name()))
        );
    }

    @Override
    public synchronized Set<String> getSubSchemaNames() {
        return metaService.getSubMetaServices().keySet();
    }

    @Override
    public Schema snapshot(SchemaVersion version) {
        return new DingoSchema(ROOT_META_SERVICE, context, ImmutableList.of(ROOT_SCHEMA_NAME));
    }
}
