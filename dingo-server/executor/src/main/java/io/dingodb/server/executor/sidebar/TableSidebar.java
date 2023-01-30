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
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.FileUtils;
import io.dingodb.common.util.NoBreakFunctions;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Parameters;
import io.dingodb.mpu.core.CoreListener;
import io.dingodb.mpu.core.CoreMeta;
import io.dingodb.mpu.core.Sidebar;
import io.dingodb.mpu.instruction.KVInstructions;
import io.dingodb.mpu.instruction.SeqInstructions;
import io.dingodb.mpu.storage.Storage;
import io.dingodb.net.Message;
import io.dingodb.net.service.ListenService;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import io.dingodb.server.executor.api.TableApi;
import io.dingodb.server.executor.store.StorageFactory;
import io.dingodb.server.executor.store.StoreInstance;
import io.dingodb.server.executor.store.StoreService;
import io.dingodb.server.protocol.MetaListenEvent;
import io.dingodb.server.executor.store.column.TypeConvert;
import io.dingodb.server.protocol.meta.Index;
import io.dingodb.server.protocol.meta.TablePart;
import io.dingodb.strorage.column.ColumnStorage;
import lombok.Getter;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static io.dingodb.common.config.DingoConfiguration.location;
import static io.dingodb.common.config.DingoConfiguration.serverId;
import static io.dingodb.common.util.ByteArrayUtils.EMPTY_BYTES;
import static io.dingodb.server.executor.config.Configuration.resolvePath;
import static io.dingodb.server.executor.sidebar.TableInstructions.UPDATE_DEFINITION;
import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;
import static io.dingodb.server.protocol.ListenerTags.MetaListener.TABLE_DEFINITION;

@Slf4j
public class TableSidebar extends BaseSidebar implements io.dingodb.store.api.StoreInstance {

    public static final CommonId PART_PREFIX = CommonId.prefix(ID_TYPE.table, TABLE_IDENTIFIER.part);
    public static final CommonId INDEX_PREFIX = CommonId.prefix(ID_TYPE.table, TABLE_IDENTIFIER.index);
    public static final CommonId TABLE_PREFIX = CommonId.prefix(ID_TYPE.table, TABLE_IDENTIFIER.table);


    public final CommonId tableId;
    @Getter
    private final List<CoreMeta> mirrors;

    private final List<TablePart> parts = new ArrayList<>();
    private final NavigableMap<byte[], TablePart> ranges = new ConcurrentSkipListMap<>(ByteArrayUtils::compare);

    @Getter
    private final Map<String, Index> indexes = new ConcurrentHashMap<>();

    private static ColumnStorage columnStorage;
    private static AtomicBoolean columnInitialized = new AtomicBoolean(false);

    @Getter
    private TableDefinition definition;

    private final Consumer<Message> definitionListener;

    @Delegate
    public final StoreInstance storeInstance;

    @Getter
    private volatile TableStatus status = TableStatus.STOPPED;

    private TableSidebar(CoreMeta meta, List<CoreMeta> mirrors, TableDefinition definition, Storage storage) {
        super(meta, mirrors, storage);
        this.tableId = meta.coreId;
        this.definition = definition;
        this.mirrors = mirrors;
        this.storeInstance = new StoreInstance(this);
        this.definitionListener = ListenService.getDefault().register(tableId, TABLE_DEFINITION);
    }

    public static TableSidebar create(
        CommonId tableId, Map<CommonId, Location> mirrors, TableDefinition definition
    ) throws Exception {
        CommonId id = new CommonId(TABLE_PREFIX.type, TABLE_PREFIX.id0, TABLE_PREFIX.id1, tableId.seq, serverId().seq);
        CoreMeta meta = new CoreMeta(id, tableId, location());
        Storage storage = StorageFactory.create(meta.label, resolvePath(tableId.toString()));
        mirrors = Parameters.cleanNull(mirrors, () -> TableApi.mirrors(CoordinatorConnector.getDefault(), tableId));
        List<CoreMeta> mirrorMetas = mirrors.entrySet().stream()
            .filter(e -> !e.getKey().equals(serverId()))
            .map(e -> new CoreMeta(
                new CommonId(id.type, id.id0, id.id1, id.domain, e.getKey().seq), tableId, e.getValue()
            ))
            .collect(Collectors.toList());
        return new TableSidebar(
            meta, mirrorMetas, definition, storage
        );
    }

    public void updateDefinition(TableDefinition definition) {
        updateDefinition(definition, true);
    }

    public void updateDefinition(TableDefinition definition, boolean sync) {
        if (sync) {
            exec(
                TableInstructions.id, UPDATE_DEFINITION, tableId, definition
            ).join();
            definitionListener.accept(new Message(ProtostuffCodec
                .write(new MetaListenEvent(MetaListenEvent.Event.UPDATE_TABLE, definition))));
        }
        this.definition = definition;
    }

    public void start(boolean sync) {
        StoreService.INSTANCE.registerStoreInstance(tableId, storeInstance);
        super.start(sync);
    }

    public boolean start() {
        StoreService.INSTANCE.registerStoreInstance(tableId, storeInstance);
        return super.start();
    }

    @Override
    public void destroy() {
        super.destroy();
        FileUtils.deleteIfExists(resolvePath(tableId.toString()));
        ListenService.getDefault().unregister(tableId, TABLE_DEFINITION);
    }

    private void initTable() {
        exec(KVInstructions.id, KVInstructions.DEL_RANGE_OC, null, null).join();
        Optional.or(
            definition.getPartDefinition(),
            this::saveDefineParts,
            () -> saveNewPart(TablePart.builder().version(1).table(tableId).start(EMPTY_BYTES).build())
        );

        Optional.ofNullable(definition.getIndexes()).filter(__ -> !__.isEmpty()).ifPresent(this::saveDefineIndexes);
        exec(KVInstructions.id, KVInstructions.SET_OC, tableId.encode(), ProtostuffCodec.write(definition)).join();
    }

    private void columnInit(String partitionId) {
        log.info("TableSidebar, columnInit, table: {}", partitionId);
        long currentTime1 = System.currentTimeMillis();
        if (this.columnInitialized.compareAndSet(false, true)) {
            log.info("TableSidebar, CK columnInit.");
            ColumnStorage.loadLibrary();
            columnStorage = new ColumnStorage();
            long currentTime2 = System.currentTimeMillis();
            log.info("TableSidebar load library, cost: {}ms.", currentTime2 - currentTime1);
            columnStorage.columnInit("/tmp/dingo_column/data_dir", "/tmp/dingo_column/log_dir");
            log.info("TableSidebar column init, cost: {}ms.", System.currentTimeMillis() - currentTime2);
        }

        List<ColumnDefinition> columns = definition.getColumns();
        int size = columns.size();
        final String[] columnNames = new String[size];
        final int[] columnTypes = new int[size];
        int i = 0;
        for (ColumnDefinition column : columns) {
            columnNames[i] = column.getName();
            int type = TypeConvert.DingoTypeToCKType(column.getType());
            columnTypes[i] = type;
            i++;
        }

        long currentTime3 = System.currentTimeMillis();
        final String sessionID = UUID.randomUUID().toString();
        this.columnStorage.createTable(sessionID, "default", partitionId.toString(), "MergeTree",
            columnNames, columnTypes);
        long currentTime4 = System.currentTimeMillis();
        log.info("TableSidebar create table: {}, cost: {}ms, total cost: {}ms.", partitionId,
            currentTime4 - currentTime3, currentTime4 - currentTime1);
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
            exec(SeqInstructions.id, 0, INDEX_PREFIX.encode()).join()
        );
        index.setId(newId);
        exec(KVInstructions.id, KVInstructions.SET_OC, newId.encode(), ProtostuffCodec.write(index)).join();
        log.info("Save new index {}", index);
    }

    private void saveNewPart(TablePart tablePart) {
        CommonId newId = new CommonId(PART_PREFIX.type(), PART_PREFIX.identifier(), tableId.seq(),
            exec(SeqInstructions.id, 0, PART_PREFIX.encode()).join()
        );
        tablePart.setId(newId);
        exec(KVInstructions.id, KVInstructions.SET_OC, newId.encode(), ProtostuffCodec.write(tablePart)).join();
        log.info("Save new part {}", tablePart);
    }

    private void startParts() {
        this.<Iterator<KeyValue>>view(
            KVInstructions.id, KVInstructions.SCAN_OC, PART_PREFIX.encode(), PART_PREFIX.encode(), true
        ).forEachRemaining(
            __ -> {
                log.info("start part");
                exec(TableInstructions.id, TableInstructions.START_PART, __.getValue()).join();
            }
        );
    }

    public void startIndexes() {
        this.<Iterator<KeyValue>>view(
            KVInstructions.id, KVInstructions.SCAN_OC, INDEX_PREFIX.encode(), INDEX_PREFIX.encode(), true
        ).forEachRemaining(
            __ -> exec(TableInstructions.id, TableInstructions.START_INDEX, __.getValue()).join()
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
            Path path = resolvePath(tableId.toString(), index.getId().toString());
            IndexSidebar indexSidebar = new IndexSidebar(this, index, meta, mirrors, path, definition.getTtl());
            addVSidebar(indexSidebar);
            log.info("Starting index {}......", id);
            CompletableFuture<Void> future = new CompletableFuture<>();
            indexSidebar.registerListener(CoreListener.primary(__ -> future.complete(null)));
            indexSidebar.registerListener(CoreListener.mirror(__ -> future.complete(null)));
            indexSidebar.registerListener(CoreListener.primary(__ -> this.indexes.put(index.getName(), index)));
            indexSidebar.registerListener(CoreListener.back(__ -> this.indexes.remove(index.getName(), index)));
            indexSidebar.start(false);
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
            if (definition.getEngine() != null && definition.getEngine().equals("MergeTree")) {
                this.columnInit(part.getId().toString());
            }
            PartitionSidebar sidebar = new PartitionSidebar(
                part, meta, mirrors, resolvePath(tableId.toString(), part.getId().toString()), definition.getTtl(),
			definition
            );
            addVSidebar(sidebar);
            log.info("Starting part {}......", id);
            CompletableFuture<Void> future = new CompletableFuture<>();
            sidebar.registerListener(CoreListener.primary(__ -> future.complete(null)));
            sidebar.registerListener(CoreListener.mirror(__ -> future.complete(null)));
            sidebar.registerListener(CoreListener.primary(__ -> ranges.put(part.getStart(), part)));
            sidebar.registerListener(CoreListener.primary(__ -> storeInstance.onPartAvailable(Part.builder()
                .start(part.getStart())
                .end(part.getEnd())
                .id(part.getId())
                .build()))
            );
            sidebar.registerListener(CoreListener.back(__ -> ranges.remove(part.getStart(), part)));
            sidebar.registerListener(CoreListener.back(__ -> storeInstance.onPartDisable(part.getStart())));
            sidebar.start(false);
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
            exec(TableInstructions.id, TableInstructions.DROP_INDEX, index).join();
            definition.removeIndex(index.getName());
            definitionListener.accept(new Message(ProtostuffCodec
                .write(new MetaListenEvent(MetaListenEvent.Event.UPDATE_TABLE, definition))));
        } else {
            indexes.remove(index.getName());
            definition.removeIndex(index.getName());
            getVCore(index.getId()).destroy();
        }
    }

    public Sidebar getPartition(byte[] start) {
        return Optional.mapOrNull(ranges.get(start), __ -> getVSidebar(__.getId()));
    }

    public Sidebar getPartition(CommonId id) {
        return getVSidebar(id);
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

    @Override
    public void primary(long clock) {
        boolean isCreate = false;
        if (view(KVInstructions.id, KVInstructions.GET_OC, tableId.encode()) == null) {
            initTable();
            isCreate = true;
        }
        setStarting();
        startParts();
        startIndexes();
        if (!isCreate) {
            definition = ProtostuffCodec
                .read((byte[]) view(KVInstructions.id, KVInstructions.GET_OC, tableId.encode()));
            storeInstance.reboot();
        }
        setRunning();
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

    @Override
    public void mirrorConnect(long clock) {
        super.mirrorConnect(clock);
        startParts();
        startIndexes();
    }
}
