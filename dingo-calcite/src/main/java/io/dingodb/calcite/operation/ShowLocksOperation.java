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

package io.dingodb.calcite.operation;

import io.dingodb.cluster.ClusterService;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.annotation.ApiDeclaration;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.Utils;
import io.dingodb.exec.transaction.base.TxnLocalData;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Table;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.transaction.api.TableLock;
import io.dingodb.transaction.api.TableLockService;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.dingodb.common.CommonId.CommonType.TRANSACTION;
import static io.dingodb.common.CommonId.CommonType.TXN_CACHE_BLOCK_LOCK;
import static io.dingodb.common.CommonId.CommonType.TXN_CACHE_LOCK;
import static io.dingodb.exec.Services.LOCAL_STORE;
import static io.dingodb.transaction.api.LockType.ROW;
import static io.dingodb.transaction.api.LockType.TABLE;

@Slf4j
public class ShowLocksOperation implements QueryOperation {

    /**
     * TODO Use job/task implement.
     */
    public interface Api {
        @ApiDeclaration
        default List<String[]> txnLocks(long tso) {
            List<String[]> locks = new ArrayList<>();
            addTxnBlock(tso, locks);
            addTxnLocked(tso, locks);
            return locks;
        }

        @ApiDeclaration
        default List<TableLock> tableLocks() {
            return TableLockService.getDefault().allTableLocks();
        }
    }

    public static final List<String> COLUMNS = Arrays.asList(
        "SERVER", "SCHEMA", "TABLE", "KEY", "TXN", "STATUS", "TYPE", "DURATION"
    );

    public static final int SERVER_INDEX = 0;
    public static final int SCHEMA_INDEX = 1;
    public static final int TABLE_INDEX = 2;
    public static final int KEY_INDEX = 3;
    public static final int TXN_INDEX = 4;
    public static final int STATUS_INDEX = 5;
    public static final int TYPE_INDEX = 6;
    public static final int DURATION_INDEX = 7;

    public static final String BLOCK = "BLOCK";
    public static final String LOCKED = "LOCKED";

    public static final String TABLE_TYPE = "TABLE";
    public static final String ROW_TYPE = "ROW";
    private static final TsoService tsoService = TsoService.getDefault();

    public final SqlIdentifier filterIdentifier;
    public final SqlKind filterKind;
    public final Object filterOperand;

    public ShowLocksOperation(SqlIdentifier filterIdentifier, SqlKind filterKind, Object filterOperand) {
        this.filterIdentifier = filterIdentifier;
        this.filterKind = filterKind;
        this.filterOperand = filterOperand;
    }

    @Override
    public List<String> columns() {
        return COLUMNS;
    }

    @Override
    public Iterator getIterator() {
        if (filterIdentifier != null && !filterIdentifier.toString().equalsIgnoreCase("duration")) {
            throw new RuntimeException("Current only supported 'duration' filter.");
        }
        List<Location> locations = ClusterService.getDefault().getComputingLocations();
        locations.remove(DingoConfiguration.location());

        long tso = tsoService.tso();
        List<String[]> locks = locations.stream()
            .map($ -> ApiRegistry.getDefault().proxy(Api.class, $))
            .flatMap($ -> {
                try {
                    return $.txnLocks(tso).stream();
                } catch (Throwable throwable) {
                    Throwable extractThrowable = Utils.extractThrowable(throwable);
                    log.error(extractThrowable.getMessage(), extractThrowable);
                    throw new RuntimeException($.toString() + " connection refused, retry in 20 seconds.");
                }
            })
            .collect(Collectors.toCollection(ArrayList::new));
        addTxnBlock(tso, locks);
        addTxnLocked(tso, locks);
        addTableLocks(tso, locks);
        return locks.stream().filter($ -> filterDuration(Long.parseLong($[DURATION_INDEX]))).iterator();
    }

    private boolean filterDuration(long duration) {
        if (filterKind == null) {
            return true;
        }
        long operand = ((Number) filterOperand).longValue();
        switch (filterKind) {
            case GREATER_THAN:
                return duration > operand;
            case GREATER_THAN_OR_EQUAL:
                return duration >= operand;
            case LESS_THAN:
                return duration < operand;
            case LESS_THAN_OR_EQUAL:
                return duration <= operand;
            case EQUALS:
                return duration == operand;
            case NOT_EQUALS:
                return duration != operand;
            default:
                throw new IllegalStateException("Unexpected value: " + filterKind);
        }
    }

    private static void addTxnBlock(long tso, List<String[]> locks) {
        Iterator<KeyValue> iterator = LOCAL_STORE.getInstance(null, null).scan(
            tso, new byte[]{(byte) TXN_CACHE_BLOCK_LOCK.code}
        );
        while (iterator.hasNext()) {
            Object[] lockKeyTuple = ByteUtils.decode(iterator.next());
            TxnLocalData txnLocalData = (TxnLocalData) lockKeyTuple[0];
            CommonId txnId = txnLocalData.getTxnId();
            CommonId tableId = txnLocalData.getTableId();
            String[] lock = new String[COLUMNS.size()];
            lock[SERVER_INDEX] = DingoConfiguration.serverId().toString();
            lock[TABLE_INDEX] = MetaService.root().getTable(tableId).name;
            lock[SCHEMA_INDEX] = getSchema(tableId);
            lock[TXN_INDEX] = txnId.toString();
            lock[STATUS_INDEX] = BLOCK;
            lock[KEY_INDEX] = lockKey(tableId, txnLocalData.getKey());
            lock[TYPE_INDEX] = ROW_TYPE;
            lock[DURATION_INDEX] = String.valueOf(tsoService.timestamp(tso) - tsoService.timestamp(txnId.seq));
            locks.add(lock);
        }
    }

    private static void addTxnLocked(long tso, List<String[]> locks) {
        Iterator<KeyValue> iterator;
        iterator = LOCAL_STORE.getInstance(null, null).scan(
            tso, new byte[]{(byte) TXN_CACHE_LOCK.code}
        );
        while (iterator.hasNext()) {
            Object[] lockKeyTuple = ByteUtils.decode(iterator.next());
            TxnLocalData txnLocalData = (TxnLocalData) lockKeyTuple[0];
            String[] lock = new String[COLUMNS.size()];
            CommonId txnId = txnLocalData.getTxnId();
            CommonId tableId = txnLocalData.getTableId();
            lock[SERVER_INDEX] = DingoConfiguration.serverId().toString();
            lock[TXN_INDEX] = txnId.toString();
            lock[TABLE_INDEX] = MetaService.root().getTable(tableId).name;
            lock[SCHEMA_INDEX] = getSchema(tableId);
            lock[STATUS_INDEX] = LOCKED;
            lock[KEY_INDEX] = lockKey(tableId, txnLocalData.getKey());
            lock[TYPE_INDEX] = ROW_TYPE;
            lock[DURATION_INDEX] = String.valueOf(tsoService.timestamp(tso) - tsoService.timestamp(txnId.seq));
            locks.add(lock);
        }
    }

    private static void addTableLocks(long tso, List<String[]> locks) {
        List<TableLock> tableLocks = TableLockService.getDefault().allTableLocks();
        List<Location> locations = ClusterService.getDefault().getComputingLocations();
        locations.remove(DingoConfiguration.location());
        tableLocks.addAll(locations.stream()
            .map($ -> ApiRegistry.getDefault().proxy(Api.class, $))
            .flatMap($ -> {
                    try {
                        return $.tableLocks().stream();
                    } catch (Throwable throwable) {
                        Throwable extractThrowable = Utils.extractThrowable(throwable);
                        log.error(extractThrowable.getMessage(), extractThrowable);
                        throw new RuntimeException($.toString() + " connection refused, retry in 20 seconds.");
                    }
                })
            .filter($ -> $.getType() == ROW)
            .collect(Collectors.toCollection(ArrayList::new)));
        tableLocks.stream().distinct().forEach(tableLock -> {
            // todo require filter?
//            if (tableLock.getType() == ROW && tableLock.lockFuture.isDone()) {
//                return;
//            }
            CompletableFuture<Boolean> lockFuture = tableLock.getLockFuture();
            String[] lock = new String[COLUMNS.size()];
            CommonId txnId = new CommonId(TRANSACTION, tableLock.lockTs, tableLock.currentTs);
            CommonId tableId = tableLock.tableId;
            lock[SERVER_INDEX] = tableLock.getServerId().toString();
            lock[TXN_INDEX] = txnId.toString();
            lock[TABLE_INDEX] = MetaService.root().getTable(tableId).name;
            lock[SCHEMA_INDEX] = getSchema(tableId);
            lock[STATUS_INDEX] = lockFuture == null ? "-" : lockFuture.isDone() && !lockFuture.isCancelled() ? LOCKED : BLOCK;
            lock[KEY_INDEX] = "-";
            lock[TYPE_INDEX] = tableLock.getType() == TABLE ? TABLE_TYPE : ROW_TYPE;
            lock[DURATION_INDEX] = String.valueOf(tsoService.timestamp(tso) - tsoService.timestamp(txnId.seq));
            locks.add(lock);
        });
    }

    private static String lockKey(CommonId tableId, byte[] keyBytes) {
        Table table = MetaService.root().getTable(tableId);
        KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(table.tupleType(), table.keyMapping());
        return Utils.buildKeyStr(table.keyMapping(), codec.decodeKeyPrefix(keyBytes));
    }

    private static String getSchema(CommonId tableId) {
        return MetaService.root().getSubMetaServices().values().stream()
            .filter($ -> $.id().seq == tableId.domain)
            .map(MetaService::name)
            .findAny()
            .orElse(null);
    }

}
