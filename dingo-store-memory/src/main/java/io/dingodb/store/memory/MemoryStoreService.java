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

package io.dingodb.store.memory;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.store.Part;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.StoreService;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

@AutoService(StoreService.class)
public class MemoryStoreService implements StoreService {

    private final Map<CommonId, MemoryStoreInstance> store = new ConcurrentHashMap<>();

    @Override
    public String name() {
        return "MEMORY";
    }

    private Part createPart(@Nonnull CommonId id, byte[] start, byte[] end) {
        return Part.builder()
            .id(id)
            .instanceId(id)
            .start(start)
            .end(end)
            .type(Part.PartType.ROW_STORE)
            .replicates(Collections.emptyList())
            .leader(new Location("localhost", 0))
            .version(0)
            .build();
    }

    @Override
    public StoreInstance getInstance(@Nonnull CommonId id) {
        MemoryStoreInstance instance = store.get(id);
        if (instance == null) {
            instance = new MemoryStoreInstance();
            instance.assignPart(createPart(id, ByteArrayUtils.EMPTY_BYTES, PrimitiveCodec.encodeVarInt(3)));
            instance.assignPart(createPart(id, PrimitiveCodec.encodeVarInt(3), PrimitiveCodec.encodeVarInt(6)));
            instance.assignPart(createPart(id, PrimitiveCodec.encodeVarInt(6), ByteArrayUtils.MAX_BYTES));
            store.put(id, instance);
        }
        return instance;
    }

    @Override
    public void deleteInstance(CommonId id) {
        store.remove(id);
    }
}
