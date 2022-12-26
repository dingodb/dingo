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
import io.dingodb.common.Location;
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.mpu.core.Core;
import io.dingodb.mpu.core.CoreMeta;
import io.dingodb.mpu.instruction.KVInstructions;
import io.dingodb.server.executor.config.Configuration;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.dingodb.mpu.instruction.KVInstructions.SET_BATCH_OC;

public class LocalMetaStore {

    public static final LocalMetaStore INSTANCE = new LocalMetaStore();

    private final Map<CommonId, TableDefinition> tableDefinitions = new ConcurrentHashMap<>();

    private final Core core;

    public LocalMetaStore() {
        try {
            CommonId id = DingoConfiguration.serverId();
            core = new Core(
                new CoreMeta(id, DingoConfiguration.location()),
                null,
                StorageFactory.create(id.toString(), Configuration.resolvePath(id.toString())),
                null
            );
            core.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void saveTable(CommonId id, TableDefinition tableDefinition) {
        core.exec(KVInstructions.id, SET_BATCH_OC, id.encode(), ProtostuffCodec.write(tableDefinition)).join();
        tableDefinitions.put(id, tableDefinition);
    }

    public TableDefinition getTable(CommonId id) {
        return tableDefinitions.get(id);
    }

    public Map<CommonId, TableDefinition> getTables() {
        return Collections.emptyMap();
    }

    public Map<CommonId, Location> getTableMirrors(CommonId commonId) {
        return null;
    }

    public void deleteTable(CommonId id) {
        core.exec(KVInstructions.id, KVInstructions.DEL_OC, id.encode());
        tableDefinitions.remove(id);
    }

}
