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
import io.dingodb.common.Location;
import io.dingodb.common.util.Optional;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.store.MetaStore;
import io.dingodb.server.protocol.meta.Replica;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;

@Slf4j
public class ReplicaAdaptor extends BaseAdaptor<Replica> {

    public static final CommonId META_ID = CommonId.prefix(ID_TYPE.table, TABLE_IDENTIFIER.replica);

    private final NavigableMap<CommonId, List<Replica>> executorReplica;

    public ReplicaAdaptor(MetaStore metaStore) {
        super(metaStore);
        MetaAdaptorRegistry.register(Replica.class, this);
        executorReplica = new ConcurrentSkipListMap<>();
        metaMap.forEach(
            (k, v) -> executorReplica.computeIfAbsent(v.getExecutor(), id -> new CopyOnWriteArrayList<>()).add(v)
        );
    }

    @Override
    protected void doSave(Replica replica) {
        // save replica
        byte[] metaContent = encodeMeta(replica);
        metaStore.upsertKeyValue(replica.getId().encode(), metaContent);
        metaMap.put(replica.getId(), replica);

        // add replica executor index
        executorReplica.computeIfAbsent(replica.getExecutor(), id -> new CopyOnWriteArrayList<>()).add(replica);
    }

    public List<Replica> getByExecutor(CommonId executor) {
        return executorReplica.get(executor);
    }

    public List<Location> getLocationsByDomain(byte[] domain) {
        return getByDomain(domain).stream().map(Replica::location).collect(Collectors.toList());
    }

    @Override
    protected CommonId newId(Replica replica) {
        byte[] partSeq = replica.getPart().seqContent();
        return new CommonId(
            META_ID.type(),
            META_ID.identifier(),
            partSeq,
            metaStore.generateSeq(CommonId.prefix(META_ID.type(), META_ID.identifier(), partSeq).encode())
        );
    }

    @Override
    public void delete(CommonId id) {
        Replica replica = metaMap.remove(id);
        Optional.ofNullable(replica)
            .ifAbsent(() -> {
                throw new RuntimeException("Not found!");
            })
            .map(Replica::getExecutor)
            .map(executorReplica::get)
            .ifPresent(replicas -> replicas.remove(replica));
    }

    @Override
    public CommonId metaId() {
        return META_ID;
    }

    @AutoService(BaseAdaptor.Creator.class)
    public static class Creator
        implements BaseAdaptor.Creator<Replica, ReplicaAdaptor> {
        @Override
        public ReplicaAdaptor create(MetaStore metaStore) {
            return new ReplicaAdaptor(metaStore);
        }
    }

}
