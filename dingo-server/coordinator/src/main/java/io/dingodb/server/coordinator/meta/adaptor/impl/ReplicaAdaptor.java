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
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.store.MetaStore;
import io.dingodb.server.protocol.meta.Replica;
import io.dingodb.store.api.KeyValue;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.INDEX_IDENTIFIER;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;

@Slf4j
public class ReplicaAdaptor extends BaseAdaptor<Replica> {

    public static final CommonId META_ID = CommonId.prefix(ID_TYPE.table, TABLE_IDENTIFIER.replica);

    private final NavigableMap<byte[], Replica> executorReplica;

    public ReplicaAdaptor(MetaStore metaStore) {
        super(metaStore);
        MetaAdaptorRegistry.register(Replica.class, this);
        Iterator<KeyValue> iterator = this.metaStore.keyValueScan(
            CommonId.prefix(ID_TYPE.index, INDEX_IDENTIFIER.replicaExecutor).encode()
        );
        executorReplica = new ConcurrentSkipListMap<>(ByteArrayUtils::compare);
        while (iterator.hasNext()) {
            KeyValue keyValue = iterator.next();
            Replica meta = decodeMeta(keyValue.getValue());
            executorReplica.put(keyValue.getKey(), meta);
        }
    }

    @Override
    protected void doSave(Replica replica) {
        CommonId executorIndex = createExecutorIndex(replica.getExecutor());
        byte[] metaContent = encodeMeta(replica);
        byte[] idContent = replica.getId().encode();
        byte[] index = new byte[CommonId.LEN * 2];
        System.arraycopy(executorIndex.encode(), 0, index, 0, CommonId.LEN);
        System.arraycopy(idContent, 0, index, CommonId.LEN, CommonId.LEN);
        metaStore.upsertKeyValue(Arrays.asList(
            new KeyValue(idContent, metaContent),
            new KeyValue(index, metaContent)
        ));
        log.info(
            "Save replica index for executor, key: {} ==> {}{}, value: {}",
            Arrays.toString(index), executorIndex, replica.getId(), replica
        );
        metaMap.put(replica.getId(), replica);
        executorReplica.put(index, replica);
    }

    private CommonId createExecutorIndex(CommonId executor) {
        return new CommonId(ID_TYPE.index, INDEX_IDENTIFIER.replicaExecutor, executor.domain(), executor.seqContent());
    }

    public List<Replica> getByExecutor(CommonId executor) {
        executor = createExecutorIndex(executor);
        byte[] start = executor.encode();
        byte[] end = executor.encode();
        end[end.length - 1]++;
        return new ArrayList<>(executorReplica.subMap(start, true, end, false).values());
    }

    @Override
    protected CommonId newId(Replica replica) {
        return new CommonId(
            META_ID.type(),
            META_ID.identifier(),
            replica.getPart().seqContent(),
            metaStore.generateSeq(CommonId.prefix(META_ID.type(), META_ID.identifier()).encode())
        );
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
