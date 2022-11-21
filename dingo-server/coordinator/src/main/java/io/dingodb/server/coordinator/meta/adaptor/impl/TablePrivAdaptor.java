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
import io.dingodb.common.privilege.TablePrivDefinition;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.store.MetaStore;
import io.dingodb.server.protocol.meta.TablePriv;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;

public class TablePrivAdaptor extends BaseAdaptor<TablePriv> {

    //todo
    public static final CommonId META_ID = CommonId.prefix(ID_TYPE.other, TABLE_IDENTIFIER.table);

    public TablePrivAdaptor(MetaStore metaStore) {
        super(metaStore);
        MetaAdaptorRegistry.register(TablePriv.class, this);
    }

    @Override
    protected CommonId newId(TablePriv meta) {
        return null;
    }

    @Override
    public CommonId metaId() {
        return META_ID;
    }

    public Map<String, List<TablePrivDefinition>> getAllDefinition() {
        return getAll().stream().map(this::metaToDefinition)
            .collect(Collectors.groupingBy(TablePrivDefinition::getUser));
    }

    private TablePrivDefinition metaToDefinition(TablePriv user) {
        return new TablePrivDefinition();
    }

    @AutoService(BaseAdaptor.Creator.class)
    public static class Creator implements BaseAdaptor.Creator<TablePriv, TablePrivAdaptor> {
        @Override
        public TablePrivAdaptor create(MetaStore metaStore) {
            return new TablePrivAdaptor(metaStore);
        }
    }

    /**
     * getTablePrivilege.
     * @param user username
     * @return list
     */
    public List<TablePrivDefinition> getTablePrivilege(String user) {
        // user dingo  table userInfo
        // user dingo  table dept
        return null;
    }
}
