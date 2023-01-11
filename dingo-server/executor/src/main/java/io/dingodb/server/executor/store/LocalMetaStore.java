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
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.mpu.core.CoreMeta;
import io.dingodb.mpu.core.Sidebar;
import io.dingodb.mpu.instruction.KVInstructions;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.dingodb.common.config.DingoConfiguration.location;
import static io.dingodb.common.config.DingoConfiguration.serverId;
import static io.dingodb.mpu.instruction.KVInstructions.SCAN_OC;
import static io.dingodb.mpu.instruction.KVInstructions.SET_OC;
import static io.dingodb.server.executor.config.Configuration.resolvePath;
import static io.dingodb.server.executor.sidebar.TableSidebar.TABLE_PREFIX;

public class LocalMetaStore extends Sidebar {

    public static final LocalMetaStore INSTANCE;

    private final Map<CommonId, TableDefinition> tableDefinitions = new ConcurrentHashMap<>();

    static {
        try {
            INSTANCE = new LocalMetaStore(serverId());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private LocalMetaStore(CommonId id) throws Exception {
        super(new CoreMeta(id, location()), StorageFactory.create(id.toString(), resolvePath(id.toString())));
        start();
    }

    public void saveTable(CommonId id, TableDefinition tableDefinition) {
        exec(KVInstructions.id, SET_OC, id.encode(), ProtostuffCodec.write(tableDefinition)).join();
        tableDefinitions.put(id, tableDefinition);
    }

    public TableDefinition getTable(CommonId id) {
        return tableDefinitions.get(id);
    }

    public Map<CommonId, TableDefinition> getTables() {
        Map<CommonId, TableDefinition> tables = new HashMap<>();
        Iterator<KeyValue> iterator = view(
            KVInstructions.id, SCAN_OC, TABLE_PREFIX.encode(), TABLE_PREFIX.encode(), true
        );
        while (iterator.hasNext()) {
            KeyValue next = iterator.next();
            tables.put(CommonId.decode(next.getKey()), ProtostuffCodec.read(next.getValue()));
        }
        return tables;
    }

    public void deleteTable(CommonId id) {
        exec(KVInstructions.id, KVInstructions.DEL_OC, id.encode());
        tableDefinitions.remove(id);
    }

}
