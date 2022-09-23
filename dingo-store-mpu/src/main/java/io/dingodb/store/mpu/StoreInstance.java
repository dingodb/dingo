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
import io.dingodb.common.Location;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.store.Part;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.FileUtils;
import io.dingodb.mpu.core.Core;
import io.dingodb.mpu.core.CoreListener;
import io.dingodb.mpu.core.CoreMeta;
import io.dingodb.mpu.core.MirrorProcessingUnit;
import io.dingodb.mpu.instruction.KVInstructions;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.ReportApi;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import io.dingodb.server.protocol.meta.TablePartStats;
import io.dingodb.server.protocol.meta.TablePartStats.ApproximateStats;
import io.dingodb.store.mpu.instruction.OpInstructions;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.STATS_IDENTIFIER;

@Slf4j
public class StoreInstance implements io.dingodb.store.api.StoreInstance {

    public final MirrorProcessingUnit mpu;
    public final Path path;
    public final String dbRocksOptionsFile;
    public final String logRocksOptionsFile;
    public Core core;

    public StoreInstance(CommonId id, Path path, final String dbRocksOptionsFile, final String logRocksOptionsFile) {
        this.path = path.toAbsolutePath();
        this.dbRocksOptionsFile = dbRocksOptionsFile;
        this.logRocksOptionsFile = logRocksOptionsFile;
        this.mpu = new MirrorProcessingUnit(id, this.path, dbRocksOptionsFile, logRocksOptionsFile);
    }

    @Override
    public synchronized void assignPart(Part part) {
        if (core != null) {
            throw new RuntimeException();
        }
        List<CoreMeta> coreMetas = new ArrayList<>();
        List<Location> replicateLocations = part.getReplicateLocations();
        List<CommonId> replicates = part.getReplicates();
        for (int i = 0; i < replicates.size(); i++) {
            coreMetas.add(i, new CoreMeta(replicates.get(i), part.getId(), mpu.id, replicateLocations.get(i), 3 - i));
        }
        core = mpu.createCore(replicates.indexOf(part.getReplicateId()), coreMetas);
        core.registerListener(CoreListener.primary(__ -> sendStats(core)));
        core.start();
    }

    public void destroy() {
        core.destroy();
        FileUtils.deleteIfExists(path);
    }

    public ApproximateStats approximateStats() {
        return new ApproximateStats(ByteArrayUtils.EMPTY_BYTES, null, approximateCount(), approximateSize());
    }

    public long approximateCount() {
        return core.storage.approximateCount();
    }

    public long approximateSize() {
        return core.storage.approximateSize();
    }

    public void sendStats(Core core) {
        try {
            if (!core.isAvailable() && core.isPrimary()) {
                return;
            }
            CommonId partId = core.meta.coreId;
            TablePartStats stats = TablePartStats.builder()
                .id(new CommonId(ID_TYPE.stats, STATS_IDENTIFIER.part, partId.domainContent(), partId.seqContent()))
                .leader(DingoConfiguration.instance().getServerId())
                .tablePart(partId)
                .table(core.meta.mpuId)
                .approximateStats(Collections.singletonList(approximateStats()))
                .time(System.currentTimeMillis())
                .build();
            ApiRegistry.getDefault().proxy(ReportApi.class, CoordinatorConnector.defaultConnector())
                .report(stats);
        } catch (Exception e) {
            log.error("{} send stats to failed.", core.meta, e);
        }
        // todo
        // if (!core.isAvailable() && core.isPrimary()) {
        //     Executors.scheduleAsync(core.meta.label + "-send-stats", () -> sendStats(core), 5, TimeUnit.SECONDS);
        // }
    }

    @Override
    public boolean exist(byte[] primaryKey) {
        return core.view(KVInstructions.id, KVInstructions.GET_OC, primaryKey) != null;
    }

    @Override
    public boolean existAny(List<byte[]> primaryKeys) {
        for (byte[] primaryKey : primaryKeys) {
            if (exist(primaryKey)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean existAny(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        return keyValueScan(startPrimaryKey, endPrimaryKey).hasNext();
    }

    @Override
    public boolean upsertKeyValue(byte[] primaryKey, byte[] row) {
        core.exec(KVInstructions.id, KVInstructions.SET_OC, primaryKey, row).join();
        return true;
    }

    @Override
    public boolean upsertKeyValue(KeyValue row) {
        core.exec(KVInstructions.id, KVInstructions.SET_OC, row.getPrimaryKey(), row.getValue()).join();
        return true;
    }

    @Override
    public boolean upsertKeyValue(List<KeyValue> rows) {
        core.exec(
            KVInstructions.id, KVInstructions.SET_BATCH_OC,
            rows.stream().flatMap(kv -> Stream.of(kv.getPrimaryKey(), kv.getValue())).toArray()
        ).join();
        return true;
    }

    @Override
    public byte[] getValueByPrimaryKey(byte[] primaryKey) {
        return core.view(KVInstructions.id, KVInstructions.GET_OC, primaryKey);
    }

    @Override
    public void deletePart(Part part) {
        core.exec(KVInstructions.id, KVInstructions.DEL_RANGE_OC, ByteArrayUtils.EMPTY_BYTES, null).join();
    }

    @Override
    public long countOrDeletePart(byte[] startKey, boolean doDeleting) {
        CompletableFuture<Long> count = CompletableFuture.supplyAsync(
            () -> core.view(KVInstructions.id, KVInstructions.COUNT_OC)
        );
        if (doDeleting) {
            core.exec(KVInstructions.id, KVInstructions.DEL_RANGE_OC, ByteArrayUtils.EMPTY_BYTES, null).join();
        }
        return count.join();
    }

    @Override
    public KeyValue getKeyValueByPrimaryKey(byte[] primaryKey) {
        return core.view(KVInstructions.id, KVInstructions.GET_OC, primaryKey);
    }

    @Override
    public List<KeyValue> getKeyValueByPrimaryKeys(List<byte[]> primaryKeys) {
        return core.view(KVInstructions.id, KVInstructions.GET_BATCH_OC, primaryKeys);
    }

    @Override
    public Iterator<KeyValue> keyValueScan() {
        return core.view(KVInstructions.id, KVInstructions.SCAN_OC);
    }

    @Override
    public Iterator<KeyValue> keyValueScan(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        isValidRangeKey(startPrimaryKey, endPrimaryKey);
        return core.view(KVInstructions.id, KVInstructions.SCAN_OC, startPrimaryKey, endPrimaryKey);
    }

    @Override
    public Iterator<KeyValue> keyValueScan(
        byte[] startPrimaryKey, byte[] endPrimaryKey, boolean includeStart, boolean includeEnd
    ) {
        isValidRangeKey(startPrimaryKey, endPrimaryKey);
        return core.view(
            KVInstructions.id, KVInstructions.SCAN_OC, startPrimaryKey, endPrimaryKey, includeStart, includeEnd
        );
    }

    @Override
    public boolean compute(byte[] startPrimaryKey, byte[] endPrimaryKey, List<byte[]> operations) {
        isValidRangeKey(startPrimaryKey, endPrimaryKey);
        core.exec(OpInstructions.id, OpInstructions.COMPUTE_OC, startPrimaryKey, endPrimaryKey, operations, -1);
        return true;
    }

    private static void isValidRangeKey(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        if (endPrimaryKey != null && ByteArrayUtils.greatThan(startPrimaryKey, endPrimaryKey)) {
            throw new IllegalArgumentException("Invalid range key, start key should be less than end key");
        }
    }

    @Override
    public boolean delete(byte[] primaryKey) {
        core.exec(KVInstructions.id, KVInstructions.DEL_OC, primaryKey).join();
        return true;
    }

    @Override
    public boolean delete(List<byte[]> primaryKeys) {
        core.exec(KVInstructions.id, KVInstructions.DEL_OC, primaryKeys.toArray()).join();
        return true;
    }

    @Override
    public boolean delete(byte[] startPrimaryKey, byte[] endPrimaryKey) {
        return core.exec(KVInstructions.id, KVInstructions.DEL_RANGE_OC, ByteArrayUtils.EMPTY_BYTES, null).join();
    }
}
