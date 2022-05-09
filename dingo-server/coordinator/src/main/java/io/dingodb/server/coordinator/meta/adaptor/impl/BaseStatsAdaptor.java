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
import io.dingodb.server.coordinator.meta.adaptor.StatsAdaptor;
import io.dingodb.server.coordinator.store.MetaStore;
import io.dingodb.server.protocol.meta.Stats;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

@Slf4j
public abstract class BaseStatsAdaptor<S extends Stats> implements StatsAdaptor<S> {

    protected final NavigableMap<CommonId, S> metaStatsMap = new ConcurrentSkipListMap<>();
    protected final MetaStore metaStatsStore;

    public BaseStatsAdaptor(MetaStore metaStatsStore) {
        this.metaStatsStore = metaStatsStore;
        Iterator<KeyValue> iterator = this.metaStatsStore.keyValueScan(statsId().encode());
        while (iterator.hasNext()) {
            S tableSegment = decodeStats(iterator.next().getValue());
            metaStatsMap.put(tableSegment.getId(), tableSegment);
        }
    }

    protected S decodeStats(byte[] content) {
        return ProtostuffCodec.read(ByteBuffer.wrap(content));
    }

    protected byte[] encodeStats(S stats) {
        return ProtostuffCodec.write(stats);
    }

    @Override
    public void onStats(S stats) {
        metaStatsStore.upsertKeyValue(stats.getId().encode(), encodeStats(stats));
        metaStatsMap.put(stats.getId(), stats);
        if (log.isDebugEnabled()) {
            log.debug("Receive stats: {}", stats);
        }
    }

    @Override
    public S getStats(CommonId id) {
        return metaStatsMap.get(id);
    }

    public List<S> getByDomain(byte[] domain) {
        CommonId prefix = statsId();
        prefix = CommonId.prefix(prefix.type(), prefix.identifier(), domain);
        ++domain[domain.length - 1];
        CommonId stop = CommonId.prefix(prefix.type(), prefix.identifier(), domain);
        return new ArrayList<>(metaStatsMap.subMap(prefix, true, stop, false).values());
    }

    @Override
    public Collection<S> getAllStats() {
        return new ArrayList<>(metaStatsMap.values());
    }

    public interface Creator<S extends Stats, A extends BaseStatsAdaptor<S>> {
        A create(MetaStore statsMetaStore);
    }

}
