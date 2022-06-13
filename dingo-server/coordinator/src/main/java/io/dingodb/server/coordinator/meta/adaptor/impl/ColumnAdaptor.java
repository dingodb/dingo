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

import io.dingodb.common.CommonId;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.store.MetaStore;
import io.dingodb.server.protocol.meta.Column;

import java.util.stream.Collectors;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;

public class ColumnAdaptor extends BaseAdaptor<Column> {

    public static final CommonId META_ID = CommonId.prefix(ID_TYPE.table, TABLE_IDENTIFIER.column);

    public ColumnAdaptor(MetaStore metaStore) {
        super(metaStore);
        MetaAdaptorRegistry.register(Column.class, this);
    }

    @Override
    public CommonId metaId() {
        return META_ID;
    }

    @Override
    protected CommonId newId(Column column) {
        byte[] tableSeq = column.getTable().seqContent();
        return new CommonId(
            META_ID.type(),
            META_ID.identifier(),
            tableSeq,
            metaStore.generateSeq(CommonId.prefix(META_ID.type(), META_ID.identifier(), tableSeq).encode())
        );
    }

    @Override
    protected void doSave(Column meta) {
    }

    public void deleteByDomain(byte[] domain) {
        CommonId prefix = metaId();
        prefix = CommonId.prefix(prefix.type(), prefix.identifier(), domain);
        ++domain[domain.length - 1];
        CommonId stop = CommonId.prefix(prefix.type(), prefix.identifier(), domain);
        metaMap.putAll(metaMap.subMap(prefix, true, stop, false).keySet().stream()
            .collect(Collectors.<CommonId, CommonId, Column>toMap(k -> k, k -> null)));
    }

    @Override
    protected void doDelete(Column column) {
        // skip
    }

}
