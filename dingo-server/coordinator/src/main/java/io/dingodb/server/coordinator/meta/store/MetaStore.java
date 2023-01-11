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

package io.dingodb.server.coordinator.meta.store;

import io.dingodb.common.store.KeyValue;
import io.dingodb.mpu.core.Sidebar;
import io.dingodb.mpu.instruction.KVInstructions;
import io.dingodb.mpu.instruction.SeqInstructions;
import io.dingodb.mpu.storage.Storage;
import io.dingodb.mpu.storage.mem.MemStorage;
import io.dingodb.mpu.storage.rocks.RocksStorage;
import io.dingodb.server.coordinator.config.Configuration;
import io.dingodb.store.api.StoreInstance;

import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class MetaStore implements StoreInstance {

    private final Sidebar sidebar;

    public MetaStore(Sidebar sidebar) {
        this.sidebar = sidebar;
    }

    public static Storage createStorage(String label) {
        if (Configuration.isMem()) {
            return new MemStorage();
        } else {
            try {
                return new RocksStorage(label, Paths.get(Configuration.dataPath(), "db"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public int generateSeq(byte[] key) {
        return sidebar.exec(SeqInstructions.id, 0, key).join();
    }

    public int generateSeq(byte[] key, int base) {
        return sidebar.exec(SeqInstructions.id, 0, key, base).join();
    }

    @Override
    public byte[] getValueByPrimaryKey(byte[] primaryKey) {
        return sidebar.view(KVInstructions.id, KVInstructions.GET_OC, primaryKey);
    }

    @Override
    public Iterator<KeyValue> keyValueScan() {
        return sidebar.view(KVInstructions.id, KVInstructions.SCAN_OC);
    }

    @Override
    public Iterator<KeyValue> keyValueScan(byte[] key) {
        return keyValueScan(key, key, true, true);
    }

    @Override
    public Iterator<KeyValue> keyValueScan(byte[] start, byte[] end, boolean includeStart, boolean includeEnd) {
        return sidebar.view(KVInstructions.id, KVInstructions.SCAN_OC, start, end, includeStart, includeEnd);
    }

    @Override
    public boolean upsertKeyValue(KeyValue row) {
        sidebar.exec(KVInstructions.id, KVInstructions.SET_OC, row.getKey(), row.getValue()).join();
        return true;
    }

    @Override
    public boolean upsertKeyValue(byte[] primaryKey, byte[] row) {
        sidebar.exec(KVInstructions.id, KVInstructions.SET_OC, primaryKey, row).join();
        return true;
    }

    @Override
    public boolean upsertKeyValue(List<KeyValue> rows) {
        sidebar.exec(
            KVInstructions.id, KVInstructions.SET_BATCH_OC,
            rows.stream().flatMap(kv -> Stream.of(kv.getPrimaryKey(), kv.getValue())).toArray()
        ).join();
        return true;
    }

    @Override
    public boolean delete(byte[] key) {
        sidebar.exec(KVInstructions.id, KVInstructions.DEL_OC, key).join();
        return true;
    }

    @Override
    public boolean delete(List<byte[]> primaryKeys) {
        sidebar.exec(KVInstructions.id, KVInstructions.DEL_BATCH_OC, primaryKeys.toArray()).join();
        return true;
    }

}
