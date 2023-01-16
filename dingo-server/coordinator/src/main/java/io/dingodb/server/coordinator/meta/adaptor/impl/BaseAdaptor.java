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
import io.dingodb.common.codec.DingoKeyValueCodec;
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.NoBreakFunctions;
import io.dingodb.server.coordinator.CoordinatorSidebar;
import io.dingodb.server.coordinator.meta.Constant;
import io.dingodb.server.coordinator.meta.adaptor.Adaptor;
import io.dingodb.server.coordinator.meta.store.MetaStore;
import io.dingodb.server.protocol.meta.Meta;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import static io.dingodb.common.util.DebugLog.debug;
import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;

@Slf4j
public abstract class BaseAdaptor<M extends Meta> implements Adaptor<M> {
    @Getter
    protected final CommonId id = CommonId
        .prefix(ID_TYPE.table, TABLE_IDENTIFIER.table, 2, Constant.ADAPTOR_SEQ_MAP.get(getClass()));
    @Getter
    protected final TableDefinition definition = Constant.ADAPTOR_DEFINITION_MAP.get(adaptFor());

    protected final NavigableMap<CommonId, M> metaMap = new ConcurrentSkipListMap<>();

    protected final DingoKeyValueCodec codec = definition.createCodec();

    protected MetaStore metaStore() {
        return CoordinatorSidebar.INSTANCE.getMetaStore();
    }

    @Override
    public CommonId id() {
        return id;
    }

    @Override
    public synchronized void reload() {
        if (metaId() == null) {
            return;
        }
        metaMap.clear();
        Iterator<KeyValue> iterator = metaStore().keyValueScan(metaId().encode());
        while (iterator.hasNext()) {
            M meta = decodeMeta(iterator.next().getValue());
            metaMap.put(meta.getId(), meta);
        }
    }

    @Override
    public String[] arrayValues(M meta) {
        Map<String, String> strValueMap = meta.strValues();
        String[] values = new String[strValueMap.size()];
        List<ColumnDefinition> columns = definition.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            values[i] = strValueMap.get(columns.get(i).getName());
        }
        return values;
    }

    protected M decodeMeta(byte[] content) {
        return ProtostuffCodec.read(ByteBuffer.wrap(content));
    }

    protected byte[] encodeMeta(M meta) {
        return ProtostuffCodec.write(meta);
    }

    protected abstract CommonId newId(M meta);

    protected void doSave(M meta) {
        metaStore().upsertKeyValue(meta.getId().encode(), encodeMeta(meta));
    }

    protected void doDelete(M meta) {
        metaStore().delete(meta.getId().encode());
    }

    @Override
    public CommonId save(M meta) {
        if (meta.getId() == null) {
            meta.setCreateTime(System.currentTimeMillis());
            meta.setId(newId(meta));
        }
        meta.setUpdateTime(System.currentTimeMillis());
        doSave(meta);
        metaMap.put(meta.getId(), meta);
        debug(log, "Save meta {}", meta);
        return meta.getId();
    }

    @Override
    public M get(CommonId id) {
        if (id == null) {
            return null;
        }
        return metaMap.get(id);
    }

    @Override
    public List<M> getByDomain(int domain) {
        return new ArrayList<>(metaMap.subMap(
            CommonId.prefix(metaId().type(), metaId().identifier(), domain), true,
            CommonId.prefix(metaId().type(), metaId().identifier(), domain + 1), false
        ).values());
    }

    @Override
    public void delete(CommonId id) {
        if (!metaMap.containsKey(id)) {
            throw new RuntimeException("Not found!");
        }
        M meta = metaMap.get(id);
        if (meta == null) {
            throw new RuntimeException("Not found " + id);
        } else {
            debug(log, "Execute delete [{}] => {}", id, meta);
        }
        doDelete(meta);
        metaMap.remove(id);
        debug(log, "Delete done [{}] => {}", id, meta);
    }

    @Override
    public Collection<M> getAll() {
        return new ArrayList<>(metaMap.values());
    }

    @Override
    public byte[] getValueByPrimaryKey(byte[] primaryKey) {
        return getKeyValueByPrimaryKey(primaryKey).getValue();
    }

    @Override
    public KeyValue getKeyValueByPrimaryKey(byte[] primaryKey) {
        try {
            return codec.encode(arrayValues(get(CommonId.decode(primaryKey))));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<KeyValue> getKeyValueByPrimaryKeys(List<byte[]> primaryKeys) {
        return primaryKeys.stream().map(this::getKeyValueByPrimaryKey).collect(Collectors.toList());
    }

    @Override
    public Iterator<KeyValue> keyValueScan() {
        return getAll().stream().map(this::arrayValues).map(NoBreakFunctions.wrap(codec::encode)).iterator();
    }

}
