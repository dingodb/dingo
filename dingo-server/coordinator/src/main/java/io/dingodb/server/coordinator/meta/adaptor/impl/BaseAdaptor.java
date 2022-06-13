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
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.server.coordinator.meta.adaptor.Adaptor;
import io.dingodb.server.coordinator.store.MetaStore;
import io.dingodb.server.protocol.meta.Meta;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static io.dingodb.common.codec.PrimitiveCodec.encodeInt;
import static io.dingodb.common.codec.PrimitiveCodec.readInt;

@Slf4j
public abstract class BaseAdaptor<M extends Meta> implements Adaptor<M> {
    protected final NavigableMap<CommonId, M> metaMap = new ConcurrentSkipListMap<>();
    protected final MetaStore metaStore;

    public BaseAdaptor(MetaStore metaStore) {
        this.metaStore = metaStore;
        Iterator<KeyValue> iterator = this.metaStore.keyValueScan(metaId().encode());
        while (iterator.hasNext()) {
            M meta = decodeMeta(iterator.next().getValue());
            metaMap.put(meta.getId(), meta);
        }
    }

    protected M decodeMeta(byte[] content) {
        return ProtostuffCodec.read(ByteBuffer.wrap(content));
    }

    protected byte[] encodeMeta(M meta) {
        return ProtostuffCodec.write(meta);
    }

    protected abstract CommonId newId(M meta);

    protected void doSave(M meta) {
        metaStore.upsertKeyValue(meta.getId().encode(), encodeMeta(meta));
    }

    protected void doDelete(M meta) {
        metaStore.delete(meta.getId().encode());
    }

    @Override
    public CommonId save(M meta) {
        if (meta.getId() ==  null) {
            meta.setCreateTime(System.currentTimeMillis());
            meta.setId(newId(meta));
        }
        meta.setUpdateTime(System.currentTimeMillis());
        doSave(meta);
        metaMap.put(meta.getId(), meta);
        log.info("Save meta {}", meta);
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
    public List<M> getByDomain(byte[] domain) {
        if (domain == null) {
            return Collections.emptyList();
        }
        CommonId from = metaId();
        from = CommonId.prefix(from.type(), from.identifier(), domain);
        CommonId to = CommonId.prefix(from.type(), from.identifier(), encodeInt(readInt(domain) + 1));
        return new ArrayList<>(metaMap.subMap(from, true, to, false).values());
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
            log.info("Execute delete [{}] => {}", id, meta);
        }
        doDelete(meta);
        metaMap.remove(id);
        log.info("Delete done [{}] => {}", id, meta);
    }

    @Override
    public Collection<M> getAll() {
        return new ArrayList<>(metaMap.values());
    }

    public interface Creator<M extends Meta, A extends BaseAdaptor<M>> {
        A create(MetaStore metaStore);
    }

}
