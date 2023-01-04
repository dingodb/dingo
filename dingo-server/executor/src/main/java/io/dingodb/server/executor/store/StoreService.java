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

package io.dingodb.server.executor.store;

import io.dingodb.common.CommonId;
import io.dingodb.common.util.FileUtils;
import io.dingodb.mpu.instruction.InstructionSetRegistry;
import io.dingodb.server.executor.store.instruction.OpInstructions;
import io.dingodb.store.api.StoreInstance;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class StoreService implements io.dingodb.store.api.StoreService {

    public static final StoreService INSTANCE = new StoreService();

    private final Map<CommonId, StoreInstance> storeInstanceMap = new ConcurrentHashMap<>();
    private final Path path = Paths.get(StoreConfiguration.dbPath());

    private StoreService() {
        FileUtils.createDirectories(path);
        InstructionSetRegistry.register(OpInstructions.id, OpInstructions.INSTRUCTIONS);
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public StoreInstance getOrCreateInstance(@NonNull CommonId id, int ttl) {
        return null;
    }

    public void registerStoreInstance(CommonId id, StoreInstance storeInstance) {
        this.storeInstanceMap.put(id, storeInstance);
    }

    @Override
    public StoreInstance getInstance(@NonNull CommonId id) {
        return storeInstanceMap.get(id);
    }

    @Override
    public void deleteInstance(CommonId id) {
        storeInstanceMap.remove(id);
    }

}
