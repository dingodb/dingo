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

package io.dingodb.exec.operator;

import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.Services;
import io.dingodb.exec.converter.ValueConverter;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.TxnPartInsertParam;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.exception.DuplicateEntryException;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NoBreakFunctions.wrap;
import static io.dingodb.exec.utils.ByteUtils.decodePessimisticKey;

@Slf4j
public class TxnPartInsertOperator extends PartModifyOperator {
    public static final TxnPartInsertOperator INSTANCE = new TxnPartInsertOperator();

    private TxnPartInsertOperator() {
    }

    @Override
    protected boolean pushTuple(Context context, Object[] tuple, Vertex vertex) {
        TxnPartInsertParam param = vertex.getParam();
        param.setContext(context);
        CommonId tableId = param.getTableId();
        CommonId txnId = vertex.getTask().getTxnId();
        CommonId partId = context.getDistribution().getId();
        DingoType schema = param.getSchema();
        StoreInstance localStore = Services.LOCAL_STORE.getInstance(tableId, partId);
        KeyValueCodec codec = param.getCodec();
        boolean isVector = false;
        if (context.getIndexId() != null) {
            Table indexTable = MetaService.root().getTable(context.getIndexId());
            List<Integer> columnIndices = param.getTable().getColumnIndices(indexTable.columns.stream()
                .map(Column::getName)
                .collect(Collectors.toList()));
            tableId = context.getIndexId();
            IndexTable index = TransactionUtil.getIndexDefinitions(tableId);
            if (index.indexType.isVector) {
                isVector = true;
            }
            if (!param.isPessimisticTxn()) {
                Object[] finalTuple = tuple;
                tuple = columnIndices.stream().map(i -> finalTuple[i]).toArray();
            }
            schema = indexTable.tupleType();
            localStore = Services.LOCAL_STORE.getInstance(context.getIndexId(), partId);
            codec = CodecService.getDefault().createKeyValueCodec(indexTable.tupleType(), indexTable.keyMapping());
        }
        StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, partId);
        Object[] newTuple = (Object[]) schema.convertFrom(tuple, ValueConverter.INSTANCE);
        KeyValue keyValue = wrap(codec::encode).apply(newTuple);
        CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
        byte[] key;
        if (isVector) {
            key = codec.encodeKeyPrefix(newTuple, 1);
            CodecService.getDefault().setId(key, partId.domain);
        } else {
            key = keyValue.getKey();
        }
        byte[] primaryLockKey = param.getPrimaryLockKey();
        byte[] txnIdByte = txnId.encode();
        byte[] tableIdByte = tableId.encode();
        byte[] partIdByte = partId.encode();
        if (param.isPessimisticTxn()) {
            byte[] keyValueKey = keyValue.getKey();
            byte[] jobIdByte = vertex.getTask().getJobId().encode();
            long forUpdateTs = vertex.getTask().getJobId().seq;
            byte[] forUpdateTsByte = PrimitiveCodec.encodeLong(forUpdateTs);
            int len = txnIdByte.length + tableIdByte.length + partIdByte.length;
            // dataKeyValue   [10_txnId_tableId_partId_a_putIf, value]
            byte[] dataKey = ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_DATA,
                keyValueKey,
                Op.PUTIFABSENT.getCode(),
                len,
                txnIdByte, tableIdByte, partIdByte);
            KeyValue oldKeyValue = localStore.get(dataKey);
            byte[] primaryLockKeyBytes = decodePessimisticKey(primaryLockKey);
            if (!(ByteArrayUtils.compare(keyValueKey, primaryLockKeyBytes, 1) == 0)) {
                // This key appears for the first time in the current transaction
                if (oldKeyValue == null) {
                    KeyValue kvKeyValue = kvStore.txnGet(TsoService.getDefault().tso(), key, param.getLockTimeOut());
                    if (kvKeyValue != null && kvKeyValue.getValue() != null) {
                        throw new DuplicateEntryException("Duplicate entry " +
                            TransactionUtil.duplicateEntryKey(tableId, key) + " for key 'PRIMARY'");
                    }
                    byte[] rollBackKey = ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, Op.DELETE, dataKey);
                    if (localStore.get(rollBackKey) != null) {
                        localStore.deletePrefix(rollBackKey);
                    }
                    // write data
                    keyValue.setKey(dataKey);
                    if ( localStore.put(keyValue)
                        && context.getIndexId() == null
                    ) {
                        param.inc();
                    }
                } else {
                    // This key appears repeatedly in the current transaction
                    repeatKey(param, keyValue, txnId, keyValueKey, localStore, dataKey,
                        tableId, context);
                }
            } else {
                // primary lock not existed ：
                // 1、first put primary lock
                if (oldKeyValue == null) {
                    byte[] rollBackKey = ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, Op.DELETE, dataKey);
                    if (localStore.get(rollBackKey) != null) {
                        localStore.deletePrefix(rollBackKey);
                    }
                    byte[] deleteKey = ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_DATA, Op.DELETE, dataKey);
                    if (localStore.get(deleteKey) != null) {
                        // delete  ->  insert  convert --> put
                        dataKey[dataKey.length - 2] = (byte) Op.PUT.getCode();
                        localStore.deletePrefix(deleteKey);
                    }
                    // first put primary lock
                    keyValue.setKey(dataKey);
                    if ( localStore.put(keyValue)
                        && context.getIndexId() == null
                    ) {
                        param.inc();
                    }
                } else {
                    // primary lock existed ：
                    repeatKey(param, keyValue, txnId, primaryLockKeyBytes, localStore, dataKey,
                        tableId, context);
                }
            }
        } else {
            byte[] insertKey = ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_DATA,
                keyValue.getKey(),
                Op.PUTIFABSENT.getCode(),
                (txnIdByte.length + tableIdByte.length + partIdByte.length),
                txnIdByte,
                tableIdByte,
                partIdByte);
            byte[] deleteKey = Arrays.copyOf(insertKey, insertKey.length);
            deleteKey[deleteKey.length - 2] = (byte) Op.DELETE.getCode();
            byte[] updateKey = Arrays.copyOf(insertKey, insertKey.length);
            updateKey[updateKey.length - 2] = (byte) Op.PUT.getCode();
            List<byte[]> bytes = new ArrayList<>(3);
            bytes.add(insertKey);
            bytes.add(deleteKey);
            bytes.add(updateKey);
            List<KeyValue> keyValues = localStore.get(bytes);
            if (keyValues != null && keyValues.size() > 0) {
                if (keyValues.size() > 1) {
                    throw new RuntimeException(txnId + " Key is not existed than two in local store");
                }
                KeyValue value = keyValues.get(0);
                byte[] oldKey = value.getKey();
                if (oldKey[oldKey.length - 2] == Op.PUTIFABSENT.getCode()
                    || oldKey[oldKey.length - 2] == Op.PUT.getCode()) {
                    throw new DuplicateEntryException("Duplicate entry " +
                        TransactionUtil.duplicateEntryKey(tableId, key) + " for key 'PRIMARY'");
                } else {
                    // delete  ->  insert  convert --> put
                    insertKey[updateKey.length - 2] = (byte) Op.PUT.getCode();
                }
            } else {
                keyValue.setKey(
                    ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_CHECK_DATA, Op.CheckNotExists, insertKey)
                );
                localStore.put(keyValue);
            }
            keyValue.setKey(
                insertKey
            );
            localStore.delete(deleteKey);
            if (localStore.put(keyValue) && context.getIndexId() == null) {
                param.inc();
                context.addKeyState(true);
            }
        }
        return true;
    }

    private static void repeatKey(TxnPartInsertParam param, KeyValue keyValue, CommonId txnId, byte[] key,
                                  StoreInstance store, byte[] dataKey,
                                  CommonId tableId, Context context) {
        // lock existed ：
        // 1、multi sql
        // 2、signal sql: insert into values(1, 'a'),(1, 'b');
        byte[] deleteKey = Arrays.copyOf(dataKey, dataKey.length);
        deleteKey[deleteKey.length - 2] = (byte) Op.DELETE.getCode();
        byte[] updateKey = Arrays.copyOf(dataKey, dataKey.length);
        updateKey[updateKey.length - 2] = (byte) Op.PUT.getCode();
        List<byte[]> bytes = new ArrayList<>(3);
        bytes.add(dataKey);
        bytes.add(deleteKey);
        bytes.add(updateKey);
        List<KeyValue> keyValues = store.get(bytes);
        if (keyValues != null && keyValues.size() > 0) {
            if (keyValues.size() > 1) {
                throw new RuntimeException(txnId + " PrimaryKey is not existed than two in local store");
            }
            KeyValue value = keyValues.get(0);
            byte[] oldKey = value.getKey();
            if (oldKey[oldKey.length - 2] == Op.PUTIFABSENT.getCode()
                || oldKey[oldKey.length - 2] == Op.PUT.getCode()) {
                throw new DuplicateEntryException("Duplicate entry " +
                    TransactionUtil.duplicateEntryKey(tableId, key) + " for key 'PRIMARY'");
            } else {
                // write data
                keyValue.setKey(dataKey);
                deleteKey[deleteKey.length - 2] = (byte) Op.DELETE.getCode();
                store.deletePrefix(deleteKey);
                if (store.put(keyValue) && context.getIndexId() == null) {
                    param.inc();
                }
            }
        } else {
            throw new RuntimeException(txnId + " PrimaryKey is not existed local store");
        }
    }

}
