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

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.codec.KeyValueCodec;
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.store.Part;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.FileUtils;
import io.dingodb.common.util.NoBreakFunctions;
import io.dingodb.common.util.Optional;
import io.dingodb.mpu.core.Core;
import io.dingodb.mpu.core.CoreListener;
import io.dingodb.mpu.core.CoreMeta;
import io.dingodb.mpu.instruction.KVInstructions;
import io.dingodb.mpu.instruction.SeqInstructions;
import io.dingodb.mpu.storage.Storage;
import io.dingodb.server.api.ServiceConnectApi;
import io.dingodb.server.executor.config.Configuration;
import io.dingodb.server.executor.store.StorageFactory;
import io.dingodb.server.executor.store.StoreInstance;
import io.dingodb.server.executor.store.StoreService;
import io.dingodb.server.protocol.meta.Index;
import io.dingodb.server.protocol.meta.TablePart;
import lombok.Getter;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.dingodb.common.config.DingoConfiguration.location;
import static io.dingodb.common.config.DingoConfiguration.serverId;
import static io.dingodb.common.util.ByteArrayUtils.EMPTY_BYTES;
import static io.dingodb.server.executor.sidebar.TableInstructions.UPDATE_DEFINITION;
import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;

@Slf4j
public class TableSidebar extends BaseSidebar implements io.dingodb.store.api.StoreInstance {

    public static final CommonId PART_PREFIX = CommonId.prefix(ID_TYPE.table, TABLE_IDENTIFIER.part);
    public static final CommonId INDEX_PREFIX = CommonId.prefix(ID_TYPE.table, TABLE_IDENTIFIER.index);
    public static final CommonId TABLE_PREFIX = CommonId.prefix(ID_TYPE.table, TABLE_IDENTIFIER.table);

    private final CompletableFuture<Void> started = new CompletableFuture<>();

    public final CommonId tableId;
    private final List<CommonId> mirrorServerIds;
    private final List<CoreMeta> mirrors;

    private final List<TablePart> parts = new ArrayList<>();
    private final NavigableMap<byte[], TablePart> ranges = new ConcurrentSkipListMap<>(ByteArrayUtils::compare);

    @Getter
    private final Map<String, Index> indexes = new ConcurrentHashMap<>();

    @Getter
    private TableDefinition definition;

    @Delegate
    public final StoreInstance storeInstance;

    @Getter
    private volatile TableStatus status = TableStatus.STOPPED;

    private TableSidebar(CoreMeta meta, List<CoreMeta> mirrors, TableDefinition definition, Storage storage) {
        super(meta, mirrors, storage);
        this.tableId = meta.coreId;
        this.definition = definition;
        this.mirrors = mirrors;
        this.mirrorServerIds = mirrors.stream()
            .map(CoreMeta::id)
            .sorted()
            .collect(Collectors.toList());
        this.storeInstance = new StoreInstance(this);
    }

    public static TableSidebar create(
        CommonId tableId, Map<CommonId, Location> mirrors, TableDefinition definition
    ) throws Exception {
        CommonId id = new CommonId(TABLE_PREFIX.type, TABLE_PREFIX.id0, TABLE_PREFIX.id1, tableId.seq, serverId().seq);
        List<CoreMeta> mirrorMetas = mirrors.entrySet().stream()
            .filter(e -> !e.getKey().equals(serverId()))
            .map(e -> new CoreMeta(
                new CommonId(id.type, id.id0, id.id1, id.domain, e.getKey().seq), tableId, e.getValue()
            ))
            .collect(Collectors.toList());
        CoreMeta meta = new CoreMeta(id, tableId, location());
        return new TableSidebar(
            meta, mirrorMetas, definition,
            StorageFactory.create(meta.label, Configuration.resolvePath(tableId.toString()))
        );
    }

    public void updateDefinition(TableDefinition definition) {
        core.exec(KVInstructions.id, KVInstructions.SET_OC, tableId.encode(), ProtostuffCodec.write(definition)).join();
        updateDefinition(definition, true);
    }

    public void updateDefinition(TableDefinition definition, boolean sync) {
        if (sync) {
            core.exec(
                TableInstructions.id, UPDATE_DEFINITION, tableId, definition
            ).join();
        }
        this.definition = definition;
    }

    public Boolean start() {
        ServiceConnectApi.INSTANCE.register(this);
        StoreService.INSTANCE.registerStoreInstance(tableId, storeInstance);
        core.start();
        started.join();
        return core.isPrimary();
    }

    @Override
    public void destroy() {
        super.destroy();
        core.destroy();
        FileUtils.deleteIfExists(Configuration.resolvePath(tableId.toString()));
    }

    private void initTable() {
        core.exec(KVInstructions.id, KVInstructions.DEL_RANGE_OC, null, null).join();
        Optional.or(
            definition.getPartDefinition(),
            this::saveDefineParts,
            () -> saveNewPart(TablePart.builder().version(1).table(tableId).start(EMPTY_BYTES).build())
        );
        Optional.ofNullable(definition.getIndexes()).filter(__ -> !__.isEmpty()).ifPresent(this::saveDefineIndexes);
        core.exec(KVInstructions.id, KVInstructions.SET_OC, tableId.encode(), ProtostuffCodec.write(definition)).join();
    }

    private void saveDefineIndexes() {
        definition.getIndexes().values().stream()
            .map(__ -> Index.builder()
                .name(__.getName()).status(__.getStatus()).table(tableId).columns(__.getColumns()).unique(__.isUnique())
                .build()
            ).forEach(this::saveNewIndex);
    }

    private void saveDefineParts() {
        String strategy = definition.getPartDefinition().getFuncName();
        int primaryKeyCount = definition.getPrimaryKeyCount();
        switch (strategy.toUpperCase()) {
            case "RANGE": {
                List<PartitionDetailDefinition> partDetailList = definition.getPartDefinition().getDetails();
                KeyValueCodec partKeyCodec = definition.createCodec();
                Iterator<byte[]> keys = partDetailList.stream()
                    .map(PartitionDetailDefinition::getOperand)
                    .map(operand -> operand.toArray(new Object[primaryKeyCount]))
                    .map(NoBreakFunctions.wrap(partKeyCodec::encodeKey))
                    .collect(Collectors.toCollection(() -> new TreeSet<>(ByteArrayUtils::compare)))
                    .iterator();
                byte [] start = EMPTY_BYTES;
                while (keys.hasNext()) {
                    saveNewPart(
                        TablePart.builder().version(1).table(tableId).start(start).end(start = keys.next()).build()
                    );
                }
                saveNewPart(TablePart.builder().version(1).table(tableId).start(start).build());
                return;
            }
            default:
                throw new IllegalStateException("Unexpected value: " + strategy.toUpperCase());
        }
    }

    public void saveNewIndex(Index index) {
        CommonId newId = new CommonId(INDEX_PREFIX.type(), INDEX_PREFIX.identifier(), tableId.seq(),
            core.exec(SeqInstructions.id, 0, INDEX_PREFIX.encode()).join()
        );
        index.setId(newId);
        core.exec(KVInstructions.id, KVInstructions.SET_OC, newId.encode(), ProtostuffCodec.write(index)).join();
        log.info("Save new index {}", index);
    }

    private void saveNewPart(TablePart tablePart) {
        CommonId newId = new CommonId(PART_PREFIX.type(), PART_PREFIX.identifier(), tableId.seq(),
            core.exec(SeqInstructions.id, 0, PART_PREFIX.encode()).join()
        );
        tablePart.setId(newId);
        core.exec(KVInstructions.id, KVInstructions.SET_OC, newId.encode(), ProtostuffCodec.write(tablePart)).join();
        log.info("Save new part {}", tablePart);
    }

    private void startParts() {
        core.<Iterator<KeyValue>>view(
            KVInstructions.id, KVInstructions.SCAN_OC, PART_PREFIX.encode(), PART_PREFIX.encode(), true
        ).forEachRemaining(
            __ -> {
                log.info("start part");
                core.exec(TableInstructions.id, TableInstructions.START_PART, __.getValue()).join();
            }
        );
    }

    public void startIndexes() {
        core.<Iterator<KeyValue>>view(
            KVInstructions.id, KVInstructions.SCAN_OC, INDEX_PREFIX.encode(), INDEX_PREFIX.encode(), true
        ).forEachRemaining(
            __ -> core.exec(TableInstructions.id, TableInstructions.START_INDEX, __.getValue()).join()
        );
    }

    public void startIndex(Index index) {
        try {
            CommonId id = index.getId();
            if (getVCore(id) != null) {
                return;
            }
            CommonId rid = new CommonId(
                ID_TYPE.table, TABLE_IDENTIFIER.index, index.getId().seq(), serverId().seq
            );
            CoreMeta meta = new CoreMeta(rid, index.getId(), location());
            List<CoreMeta> mirrors = this.mirrors.stream()
                .map(mirror -> new CoreMeta(
                    new CommonId(ID_TYPE.table, TABLE_IDENTIFIER.index, index.getId().seq(), mirror.id.seq()),
                    index.getId(),
                    mirror.location))
                .collect(Collectors.toList());
            Core vCore = new IndexSidebar(
                this, index, meta, mirrors, Configuration.resolvePath(tableId.toString(), index.getId().toString())
            ).getCore();
            addVCore(vCore);
            log.info("Starting index {}......", id);
            CompletableFuture<Void> future = new CompletableFuture<>();
            vCore.registerListener(CoreListener.primary(__ -> future.complete(null)));
            vCore.registerListener(CoreListener.mirror(__ -> future.complete(null)));
            vCore.registerListener(CoreListener.primary(__ -> this.indexes.put(index.getName(), index)));
            vCore.registerListener(CoreListener.back(__ -> this.indexes.remove(index.getName(), index)));
            vCore.start();
            future.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Start index {} error.", index, e);
        }
    }

    public void startPartition(TablePart part) {
        try {
            CommonId id = part.getId();
            if (getVCore(id) != null) {
                return;
            }
            parts.add(part);
            CommonId rid = new CommonId(
                ID_TYPE.table, TABLE_IDENTIFIER.part, part.getId().seq(), serverId().seq
            );
            CoreMeta meta = new CoreMeta(rid, part.getId(), location());
            List<CoreMeta> mirrors = this.mirrors.stream()
                .map(mirror -> new CoreMeta(
                    new CommonId(ID_TYPE.table, TABLE_IDENTIFIER.part, part.getId().seq(), mirror.id.seq()),
                    part.getId(),
                    mirror.location))
                .collect(Collectors.toList());
            Core vCore = new PartitionSidebar(
                part, meta, mirrors, Configuration.resolvePath(tableId.toString(), part.getId().toString())
            ).getCore();
            addVCore(vCore);
            log.info("Starting part {}......", id);
            CompletableFuture<Void> future = new CompletableFuture<>();
            vCore.registerListener(CoreListener.primary(__ -> future.complete(null)));
            vCore.registerListener(CoreListener.mirror(__ -> future.complete(null)));
            vCore.registerListener(CoreListener.primary(__ -> ranges.put(part.getStart(), part)));
            vCore.registerListener(CoreListener.primary(__ -> storeInstance.onPartAvailable(Part.builder()
                .start(part.getStart())
                .end(part.getEnd())
                .id(part.getId())
                .build()))
            );
            vCore.registerListener(CoreListener.back(__ -> ranges.remove(part.getStart(), part)));
            vCore.registerListener(CoreListener.back(__ -> storeInstance.onPartDisable(part.getStart())));
            vCore.start();
            future.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Start index {} error.", part, e);
        }
    }

    public void dropIndex(String name) {
        if (status == TableStatus.RUNNING) {
            dropIndex(indexes.get(name), true);
        } else {
            throw new RuntimeException("Table status not running.");
        }
    }

    public void dropIndex(Index index, boolean sync) {
        if (sync) {
            core.exec(TableInstructions.id, TableInstructions.DROP_INDEX, index).join();
        } else {
            indexes.remove(index.getName());
            getVCore(index.getId()).destroy();
        }
    }

    public Core getPartition(byte[] start) {
        return Optional.mapOrNull(ranges.get(start), __ -> getVCore(__.getId()));
    }

    public Core getPartition(CommonId id) {
        return getVCore(id);
    }

    public boolean ttl() {
        return definition.getTtl() > 0;
    }

    public StoreInstance storeInstance() {
        return storeInstance;
    }

    public List<TablePart> partitions() {
        return parts;
    }

    public void primary(long clock) {
        if (core.view(KVInstructions.id, KVInstructions.GET_OC, tableId.encode()) == null) {
            initTable();
        }
        setStarting();
        startParts();
        startIndexes();
        storeInstance.reboot();
        started.complete(null);
        setRunning();
    }

    @Override
    public void back(long clock) {

    }

    @Override
    public void mirror(long clock) {
        started.complete(null);
    }

    @Override
    public void losePrimary(long clock) {
    }

    public void setStarting() {
        this.status = TableStatus.STARTING;
    }

    public void setBusy() {
        this.status = TableStatus.BUSY;
    }

    public void setRunning() {
        this.status = TableStatus.RUNNING;
    }

    public void setStopping() {
        this.status = TableStatus.STOPPING;
    }

    public void setStopped() {
        this.status = TableStatus.STOPPED;
    }
}
