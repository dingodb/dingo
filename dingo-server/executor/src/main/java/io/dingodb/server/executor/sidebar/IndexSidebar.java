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

package io.dingodb.server.executor.sidebar;

import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.Utils;
import io.dingodb.mpu.core.CoreMeta;
import io.dingodb.mpu.instruction.KVInstructions;
import io.dingodb.server.executor.store.StorageFactory;
import io.dingodb.server.protocol.meta.Index;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import static io.dingodb.common.codec.PrimitiveCodec.encodeInt;
import static io.dingodb.common.util.ByteArrayUtils.unsliced;

public class IndexSidebar extends BaseSidebar implements io.dingodb.store.api.StoreInstance {

    private final Index index;
    private final TableSidebar tableSidebar;

    public IndexSidebar(
        TableSidebar tableSidebar, Index index, CoreMeta meta, List<CoreMeta> mirrors, Path path
    ) throws Exception {
        super(meta, mirrors, StorageFactory.create(meta.label, path));
        this.tableSidebar = tableSidebar;
        this.index = index;
    }

    @Override
    public boolean upsertKeyValue(byte[] primaryKey, byte[] row) {
        if (tableSidebar.ttl()) {
            core.exec(
                KVInstructions.id,
                KVInstructions.SET_OC,
                primaryKey,
                encodeInt(Utils.currentSecond(), unsliced(row, 0, row.length + 4), row.length, false)
            ).join();
        } else {
            core.exec(KVInstructions.id, KVInstructions.SET_OC, primaryKey, row).join();
        }

        return true;
    }

    @Override
    public byte[] getValueByPrimaryKey(byte[] primaryKey) {
        return core.view(KVInstructions.id, KVInstructions.GET_OC, primaryKey);
    }

    @Override
    public Iterator<KeyValue> keyValueScan() {
        return core.view(KVInstructions.id, KVInstructions.SCAN_OC);
    }

    @Override
    public Iterator<KeyValue> keyValueScan(
        byte[] startPrimaryKey, byte[] endPrimaryKey, boolean includeStart, boolean includeEnd
    ) {
        return core
            .view(KVInstructions.id, KVInstructions.SCAN_OC, startPrimaryKey, endPrimaryKey, includeStart, includeEnd);
    }

    @Override
    public void primary(long clock) {
        super.primary(clock);
    }

    @Override
    public void back(long clock) {
        super.back(clock);
    }

    @Override
    public void mirror(long clock) {
        super.mirror(clock);
    }

    @Override
    public void losePrimary(long clock) {
        super.losePrimary(clock);
    }

}
