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
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.converter.DingoConverter;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.Services;
import io.dingodb.exec.converter.ValueConverter;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.TxnPartUpdateParam;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NoBreakFunctions.wrap;
import static io.dingodb.exec.utils.ByteUtils.decodePessimisticKey;

@Slf4j
public class TxnPartUpdateOperator extends PartModifyOperator {
    public static final TxnPartUpdateOperator INSTANCE = new TxnPartUpdateOperator();

    private TxnPartUpdateOperator() {
    }

    @Override
    protected boolean pushTuple(Context context, Object[] tuple, Vertex vertex) {
        TxnPartUpdateParam param = vertex.getParam();
        param.setContext(context);
        DingoType schema = param.getSchema();
        TupleMapping mapping = param.getMapping();
        List<SqlExpr> updates = param.getUpdates();

        int tupleSize = schema.fieldCount();
        Object[] newTuple = Arrays.copyOf(tuple, tupleSize);
        Object[] copyTuple = Arrays.copyOf(tuple, tuple.length);
        boolean updated = false;
        int i = 0;
        try {
            for (i = 0; i < mapping.size(); ++i) {
                Object newValue = updates.get(i).eval(tuple);
                int index = mapping.get(i);
                if ((newTuple[index] == null && newValue != null)
                    || (newTuple[index] != null && !newTuple[index].equals(newValue))
                ) {
                    newTuple[index] = newValue;
                    updated = true;
                }
            }
            if (param.isHasAutoInc() && param.getAutoIncColIdx() < tuple.length) {
                long autoIncVal = Long.parseLong(newTuple[param.getAutoIncColIdx()].toString());
                MetaService metaService = MetaService.root();
                metaService.updateAutoIncrement(param.getTableId(), autoIncVal);
            }
            CommonId txnId = vertex.getTask().getTxnId();
            CommonId tableId = param.getTableId();
            CommonId partId = context.getDistribution().getId();
            KeyValueCodec codec = param.getCodec();
            boolean calcPartId = false;
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
                schema = indexTable.tupleType();
                codec = CodecService.getDefault().createKeyValueCodec(indexTable.tupleType(), indexTable.keyMapping());
                Object[] finalNewTuple = newTuple;
                newTuple = columnIndices.stream().map(c -> finalNewTuple[c]).toArray();
                Object[] copyNewTuple = copyTuple;
                copyTuple = columnIndices.stream().map(c -> copyNewTuple[c]).toArray();
                if (updated && columnIndices.stream().anyMatch(c -> mapping.contains(c))) {
                    PartitionService ps = PartitionService.getService(
                        Optional.ofNullable(indexTable.getPartitionStrategy())
                            .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
                    byte[] key = wrap(codec::encodeKey).apply(newTuple);
                    partId = ps.calcPartId(key, MetaService.root().getRangeDistribution(tableId));
                    log.info("{} update index primary key is{} calcPartId is {}",
                        txnId,
                        Arrays.toString(key),
                        partId
                    );
                    calcPartId = true;
                }
            }
            Object[] newTuple2 = (Object[]) schema.convertFrom(newTuple, ValueConverter.INSTANCE);

            byte[] key = wrap(codec::encodeKey).apply(newTuple2);
            CodecService.getDefault().setId(key, partId.domain);
            byte[] vectorKey;
            if (isVector) {
                vectorKey = codec.encodeKeyPrefix(newTuple2, 1);
                CodecService.getDefault().setId(vectorKey, partId.domain);
            } else {
                vectorKey = key;
            }
            StoreInstance localStore = Services.LOCAL_STORE.getInstance(tableId, partId);
            StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, partId);
            byte[] primaryLockKey = param.getPrimaryLockKey();
            byte[] txnIdBytes = vertex.getTask().getTxnId().encode();
            byte[] tableIdBytes = tableId.encode();
            byte[] partIdBytes = partId.encode();
            if (param.isPessimisticTxn()) {
                byte[] jobIdByte = vertex.getTask().getJobId().encode();
                long forUpdateTs = vertex.getTask().getJobId().seq;
                byte[] forUpdateTsByte = PrimitiveCodec.encodeLong(forUpdateTs);
                int len = txnIdBytes.length + tableIdBytes.length + partIdBytes.length;
                // dataKeyValue   [10_txnId_tableId_partId_a_putIf, value]
                byte[] dataKey = ByteUtils.encode(
                    CommonId.CommonType.TXN_CACHE_DATA,
                    key,
                    Op.PUT.getCode(),
                    len,
                    txnIdBytes,
                    tableIdBytes,
                    partIdBytes
                );
                KeyValue oldKeyValue = localStore.get(dataKey);
                byte[] primaryLockKeyBytes = decodePessimisticKey(primaryLockKey);
                if (log.isDebugEnabled()) {
                    log.info("{} updated is true key is {}", txnId, Arrays.toString(key));
                }
                if (!updated) {
                    log.warn("{} updated is false key is {}", txnId, Arrays.toString(key));
                    // data is not exist local store
                    if (oldKeyValue == null) {
                        byte[] rollBackKey = ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, Op.DELETE, dataKey);
                        KeyValue rollBackKeyValue = new KeyValue(rollBackKey, null);
                        log.info("{}, updated is false residual key is:{}",
                            txnId, Arrays.toString(rollBackKey));
                        localStore.put(rollBackKeyValue);
                    }
                    return true;
                }
                if (!(ByteArrayUtils.compare(key, primaryLockKeyBytes, 1) == 0)) {
                    // This key appears for the first time in the current transaction
                    if (oldKeyValue == null) {
                        if (calcPartId) {
                            KeyValue keyValue = wrap(codec::encode).apply(newTuple2);
                            CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
                            // write data
                            keyValue.setKey(dataKey);
                            localStore.put(keyValue);
                            return true;
                        }
                        KeyValue kvKeyValue = kvStore.txnGet(TsoService.getDefault().tso(), vectorKey, param.getLockTimeOut());
                        if (kvKeyValue == null || kvKeyValue.getValue() == null) {
                            byte[] rollBackKey = ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, Op.DELETE, dataKey);
                            KeyValue rollBackKeyValue = new KeyValue(rollBackKey, null);
                            log.info("{}, update key is not primaryLock residual key is:{}",
                                txnId, Arrays.toString(rollBackKey));
                            localStore.put(rollBackKeyValue);
                            return true;
                        }
                        KeyValue keyValue = wrap(codec::encode).apply(newTuple2);
                        CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
                        // write data
                        keyValue.setKey(dataKey);
                        if (localStore.put(keyValue)
                            && context.getIndexId() == null
                        ) {
                            param.inc();
                        }
                    } else {
                        KeyValue keyValue = wrap(codec::encode).apply(newTuple2);
                        CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
                        // This key appears repeatedly in the current transaction
                        repeatKey(param, keyValue, txnId, localStore, dataKey,
                             context, updated);
                    }
                } else {
                    // primary lock not existed ：
                    // 1、first put primary lock
                    if (oldKeyValue == null) {
                        // first put primary lock
                        KeyValue kvKeyValue = kvStore.txnGet(TsoService.getDefault().tso(), vectorKey, param.getLockTimeOut());
                        if (kvKeyValue == null || kvKeyValue.getValue() == null) {
                            byte[] rollBackKey = ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, Op.DELETE, dataKey);
                            KeyValue rollBackKeyValue = new KeyValue(rollBackKey, null);
                            log.info("{}, update first put primary lock residual key is:{}",
                                txnId, Arrays.toString(rollBackKey));
                            localStore.put(rollBackKeyValue);
                            return true;
                        }
                        KeyValue keyValue = wrap(codec::encode).apply(newTuple2);
                        CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
                        // write data
                        keyValue.setKey(dataKey);
                        if (localStore.put(keyValue)
                            && context.getIndexId() == null
                        ) {
                            param.inc();
                        }
                    } else {
                        KeyValue keyValue = wrap(codec::encode).apply(newTuple2);
                        CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
                        // primary lock existed ：
                        repeatKey(param, keyValue, txnId, localStore, dataKey,
                            context, updated);
                    }
                }
            } else {
                KeyValue keyValue = wrap(codec::encode).apply(newTuple2);
                CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
                log.info("{} update key is {}, partId is {}", txnId, Arrays.toString(keyValue.getKey()), partId);
                if (calcPartId) {
                    // begin insert update commit
                    byte[] oldKey = wrap(codec::encodeKey).apply(copyTuple);
                    CodecService.getDefault().setId(oldKey, context.getDistribution().getId().domain);
                    if (!ByteArrayUtils.equal(keyValue.getKey(), oldKey)) {
                        localStore = Services.LOCAL_STORE.getInstance(tableId, context.getDistribution().getId());
                        byte[] oldDataKey = ByteUtils.encode(
                            CommonId.CommonType.TXN_CACHE_DATA,
                            oldKey,
                            Op.PUT.getCode(),
                            (txnIdBytes.length + tableIdBytes.length + partIdBytes.length),
                            txnIdBytes,
                            tableIdBytes,
                            context.getDistribution().getId().encode());
                        localStore.delete(oldDataKey);
                        byte[] updateKey = Arrays.copyOf(oldDataKey, oldDataKey.length);
                        updateKey[updateKey.length - 2] = (byte) Op.PUTIFABSENT.getCode();
                        localStore.delete(updateKey);
                        byte[] deleteKey = Arrays.copyOf(oldDataKey, oldDataKey.length);
                        deleteKey[deleteKey.length - 2] = (byte) Op.DELETE.getCode();
                        localStore.put(new KeyValue(deleteKey, wrap(codec::encode).apply(copyTuple).getValue()));
                        localStore = Services.LOCAL_STORE.getInstance(tableId, partId);
                    }
                }
                keyValue.setKey(
                    ByteUtils.encode(
                        CommonId.CommonType.TXN_CACHE_DATA,
                        keyValue.getKey(),
                        Op.PUT.getCode(),
                        (txnIdBytes.length + tableIdBytes.length + partIdBytes.length),
                        txnIdBytes,
                        tableIdBytes,
                        partIdBytes)
                );
                byte[] insertKey = Arrays.copyOf(keyValue.getKey(), keyValue.getKey().length);
                insertKey[insertKey.length - 2] = (byte) Op.PUTIFABSENT.getCode();
                localStore.delete(insertKey);
                if (updated) {
                    localStore.delete(keyValue.getKey());
                    if (localStore.put(keyValue) && context.getIndexId() == null) {
                        param.inc();
                        context.addKeyState(true);
                    }
                }
            }
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
        return true;
    }

    private static void repeatKey(TxnPartUpdateParam param, KeyValue keyValue, CommonId txnId,
                                  StoreInstance store, byte[] dataKey, Context context, boolean updated) {
        // lock existed ：
        // 1、multi sql
        byte[] deleteKey = Arrays.copyOf(dataKey, dataKey.length);
        deleteKey[deleteKey.length - 2] = (byte) Op.DELETE.getCode();
        byte[] insertKey = Arrays.copyOf(dataKey, dataKey.length);
        insertKey[insertKey.length - 2] = (byte) Op.PUTIFABSENT.getCode();
        List<byte[]> bytes = new ArrayList<>(3);
        bytes.add(dataKey);
        bytes.add(deleteKey);
        bytes.add(insertKey);
        List<KeyValue> keyValues = store.get(bytes);
        if (keyValues != null && keyValues.size() > 0) {
            if (keyValues.size() > 1) {
                throw new RuntimeException(txnId + " PrimaryKey is not existed than two in local store");
            }
            // write data
            keyValue.setKey(dataKey);
            store.deletePrefix(deleteKey);
            store.deletePrefix(insertKey);
            if (updated) {
                store.deletePrefix(dataKey);
                if (store.put(keyValue) && context.getIndexId() == null) {
                    param.inc();
                }
            }
        } else {
            throw new RuntimeException(txnId + " PrimaryKey is not existed local store");
        }
    }
}
