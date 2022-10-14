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
import io.dingodb.common.codec.KeyValueCodec;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.store.Part;
import io.dingodb.common.table.DingoKeyValueCodec;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.UdfUtils;
import io.dingodb.mpu.core.Core;
import io.dingodb.mpu.core.CoreListener;
import io.dingodb.mpu.core.CoreMeta;
import io.dingodb.mpu.core.MirrorProcessingUnit;
import io.dingodb.mpu.instruction.KVInstructions;
import io.dingodb.mpu.storage.rocks.RocksUtils;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.MetaServiceApi;
import io.dingodb.server.api.ReportApi;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import io.dingodb.server.protocol.meta.TablePartStats;
import io.dingodb.server.protocol.meta.TablePartStats.ApproximateStats;
import io.dingodb.store.mpu.instruction.OpInstructions;
import lombok.extern.slf4j.Slf4j;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.jse.JsePlatform;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.STATS_IDENTIFIER;

@Slf4j
public class StoreInstance implements io.dingodb.store.api.StoreInstance {

    private final CommonId id;
    public final MirrorProcessingUnit mpu;
    public final Path path;
    public Core core;
    private int ttl;
    private Map<String, Globals> globalsMap = new HashMap<>();
    private Map<String, TableDefinition> definitionMap = new HashMap<>();
    private Map<String, KeyValueCodec> codecMap = new HashMap<>();

    public StoreInstance(CommonId id, Path path) {
        this(id, path, "", "", -1);
    }

    public StoreInstance(CommonId id, Path path, final String dbRocksOptionsFile, final String logRocksOptionsFile,
                         final int ttl) {
        this.id = id;
        this.path = path.toAbsolutePath();
        this.ttl = ttl;
        this.mpu = new MirrorProcessingUnit(id, this.path, dbRocksOptionsFile, logRocksOptionsFile, this.ttl);
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
        /**
         * to avoid file handle leak  when drop table
         */
        // FileUtils.deleteIfExists(path);
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
        if (RocksUtils.ttlValid(this.ttl)) {
            core.exec(KVInstructions.id, KVInstructions.SET_OC, primaryKey, RocksUtils.getValueWithNowTs(row)).join();
        } else {
            core.exec(KVInstructions.id, KVInstructions.SET_OC, primaryKey, row).join();
        }
        return true;
    }

    @Override
    public boolean upsertKeyValue(KeyValue row) {
        return upsertKeyValue(row.getPrimaryKey(), row.getValue());
    }

    @Override
    public boolean upsertKeyValue(List<KeyValue> rows) {
        List<KeyValue> kvList;
        if (RocksUtils.ttlValid(this.ttl)) {
            kvList = RocksUtils.getValueWithNowTsList(rows);
        } else {
            kvList = rows;
        }
        core.exec(
            KVInstructions.id, KVInstructions.SET_BATCH_OC,
            kvList.stream().flatMap(kv -> Stream.of(kv.getPrimaryKey(), kv.getValue())).toArray()
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
    public long countDeleteByRange(byte[] startKey, byte[] endKey) {
        CompletableFuture<Long> count = CompletableFuture.supplyAsync(
            () -> core.view(KVInstructions.id, KVInstructions.COUNT_OC)
        );
        core.exec(KVInstructions.id, KVInstructions.DEL_RANGE_OC, ByteArrayUtils.EMPTY_BYTES, null).join();
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
        int timestamp = -1;
        if (RocksUtils.ttlValid(this.ttl)) {
            timestamp = RocksUtils.getCurrentTimestamp();
        }
        core.exec(OpInstructions.id, OpInstructions.COMPUTE_OC, startPrimaryKey, endPrimaryKey, operations,
            timestamp).join();
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

    @Override
    public KeyValue udfGet(byte[] primaryKey, String udfName, String functionName, int version) {
        try {
            KeyValue keyValue = new KeyValue(primaryKey, getValueByPrimaryKey(primaryKey));
            String cacheKey = udfName + "-" + version;
            MetaServiceApi metaServiceApi
                = ApiRegistry.getDefault().proxy(MetaServiceApi.class, CoordinatorConnector.defaultConnector());
            if (!codecMap.containsKey(cacheKey)) {
                TableDefinition tableDefinition = metaServiceApi.getTableDefinition(id);
                DingoKeyValueCodec codec =
                    new DingoKeyValueCodec(tableDefinition.getDingoType(), tableDefinition.getKeyMapping());
                definitionMap.put(cacheKey, tableDefinition);
                codecMap.put(cacheKey, codec);
            }
            KeyValueCodec codec = codecMap.get(cacheKey);
            Object[] record = codec.decode(keyValue);
            TableDefinition definition = definitionMap.get(cacheKey);
            LuaTable table = UdfUtils.getLuaTable(definition.getDingoSchema(), record);
            if (!globalsMap.containsKey(cacheKey)) {
                String function = metaServiceApi.getUDF(id, udfName, version);
                if (function != null) {
                    Globals globals = JsePlatform.standardGlobals();
                    globals.load(function).call();
                    globalsMap.put(cacheKey, globals);
                } else {
                    definitionMap.remove(cacheKey);
                    codecMap.remove(cacheKey);
                    throw new RuntimeException("UDF not register");
                }
            }
            Globals globals = globalsMap.get(cacheKey);
            LuaValue udf = globals.get(LuaValue.valueOf(functionName));
            LuaValue result = udf.call(table);
            record = UdfUtils.getObject(definition.getDingoSchema(), result);
            return codec.encode(record);
        } catch (Exception e) {
            throw new RuntimeException("UDF ERROR:", e);
        }
    }

    @Override
    public boolean udfUpdate(byte[] primaryKey, String udfName, String functionName, int version) {
        KeyValue updatedKeyValue = udfGet(primaryKey, udfName, functionName, version);
        return upsertKeyValue(updatedKeyValue);
    }
}
