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

package io.dingodb.store.mpu;

import io.dingodb.common.CommonId;
import io.dingodb.common.util.FileUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.mpu.instruction.InstructionSetRegistry;
import io.dingodb.store.mpu.instruction.OpInstructions;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

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
    public StoreInstance getOrCreateInstance(@Nonnull CommonId id) {
        Path instancePath = Paths.get(StoreConfiguration.dbPath(), id.toString());
        return storeInstanceMap.compute(id, (l, i) -> i == null ? new StoreInstance(id, instancePath,
            StoreConfiguration.dbRocksOptionsFile(), StoreConfiguration.logRocksOptionsFile()) : i);
    }

    @Override
    public StoreInstance getInstance(@Nonnull CommonId id) {
        return storeInstanceMap.get(id);
    }

    @Override
    public void deleteInstance(CommonId id) {
        Optional.ifPresent(storeInstanceMap.remove(id), StoreInstance::destroy);
    }

}
