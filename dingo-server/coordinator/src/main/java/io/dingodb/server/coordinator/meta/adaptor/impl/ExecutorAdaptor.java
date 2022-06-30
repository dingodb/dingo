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
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.schedule.processor.TableStoreProcessor;
import io.dingodb.server.coordinator.store.MetaStore;
import io.dingodb.server.protocol.meta.Executor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.SERVICE_IDENTIFIER;
import static io.dingodb.server.protocol.CommonIdConstant.ZERO_DOMAIN;

public class ExecutorAdaptor extends BaseAdaptor<Executor> {

    public static final CommonId META_ID = CommonId.prefix(ID_TYPE.service, SERVICE_IDENTIFIER.executor);

    protected final Map<Location, Executor> locationMap = new ConcurrentHashMap<>();

    public ExecutorAdaptor(MetaStore metaStore) {
        super(metaStore);
        MetaAdaptorRegistry.register(Executor.class, this);
        metaMap.values().forEach(__ -> locationMap.put(__.location(), __));
        metaMap.values().forEach(__ -> TableStoreProcessor.addStore(__.getId(), __.location()));
    }

    @Override
    protected void doSave(Executor meta) {
        super.doSave(meta);
        locationMap.put(meta.location(), meta);
    }

    @Override
    protected void doDelete(Executor meta) {
        super.doDelete(meta);
        locationMap.remove(meta.location());
    }

    @Override
    public CommonId metaId() {
        return META_ID;
    }

    public CommonId newId(Executor executor) {
        return new CommonId(
            META_ID.type(),
            META_ID.identifier(),
            ZERO_DOMAIN,
            metaStore.generateSeq(CommonId.prefix(META_ID.type(), META_ID.identifier()).encode())
        );
    }

    public Executor get(Location location) {
        return locationMap.get(location);
    }

    public Location getLocation(CommonId id) {
        return get(id).location();
    }

    @AutoService(BaseAdaptor.Creator.class)
    public static class Creator implements BaseAdaptor.Creator<Executor, ExecutorAdaptor> {
        @Override
        public ExecutorAdaptor create(MetaStore metaStore) {
            return new ExecutorAdaptor(metaStore);
        }
    }
}
