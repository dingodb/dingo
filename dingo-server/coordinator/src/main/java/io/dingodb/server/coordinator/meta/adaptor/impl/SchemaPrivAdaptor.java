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

package io.dingodb.server.coordinator.meta.adaptor.impl;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.common.privilege.SchemaPrivDefinition;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.store.MetaStore;
import io.dingodb.server.protocol.meta.SchemaPriv;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;

public class SchemaPrivAdaptor extends BaseAdaptor<SchemaPriv> {
    //todo
    public static final CommonId META_ID = CommonId.prefix(ID_TYPE.other, TABLE_IDENTIFIER.table);

    public SchemaPrivAdaptor(MetaStore metaStore) {
        super(metaStore);
        MetaAdaptorRegistry.register(SchemaPriv.class, this);
    }

    @Override
    protected CommonId newId(SchemaPriv meta) {
        return null;
    }

    @Override
    public CommonId metaId() {
        return META_ID;
    }

    public Map<String, List<SchemaPrivDefinition>> getAllDefinition() {
        return getAll().stream().map(this::metaToDefinition)
            .collect(Collectors.groupingBy(SchemaPrivDefinition::getUsername));
    }

    private SchemaPrivDefinition metaToDefinition(SchemaPriv tablePriv) {
        return new SchemaPrivDefinition();
    }

    @AutoService(BaseAdaptor.Creator.class)
    public static class Creator implements BaseAdaptor.Creator<SchemaPriv, SchemaPrivAdaptor> {
        @Override
        public SchemaPrivAdaptor create(MetaStore metaStore) {
            return new SchemaPrivAdaptor(metaStore);
        }
    }

    public List<SchemaPrivDefinition> getSchemaPrivilege(String user) {
        return null;
    }
}
