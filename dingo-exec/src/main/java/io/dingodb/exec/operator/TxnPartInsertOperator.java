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
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.Services;
import io.dingodb.exec.converter.ValueConverter;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.FinWithProfiles;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.TxnPartInsertParam;
import io.dingodb.exec.transaction.base.TxnPartData;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.exec.utils.OpStateUtils;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.IndexType;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.exception.DuplicateEntryException;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public class TxnPartInsertOperator extends PartModifyOperator {
    public static final TxnPartInsertOperator INSTANCE = new TxnPartInsertOperator();

    private TxnPartInsertOperator() {
    }

    @Override
    protected boolean pushTuple(Context context, Object[] tuple, Vertex vertex) {
        TxnPartInsertParam param = vertex.getParam();
        OperatorProfile profile = param.getProfile("partInsertLocal");
        long start = System.currentTimeMillis();
        if (param.isHasAutoInc() && param.getAutoIncColIdx() < tuple.length) {
            Object tmp = tuple[param.getAutoIncColIdx()];
            if (tmp instanceof Long || tmp instanceof Integer) {
                long autoIncVal = Long.parseLong(tmp.toString());
                MetaService metaService = MetaService.root();
                metaService.updateAutoIncrement(param.getTableId(), autoIncVal);
                param.getAutoIncList().add(autoIncVal);
            }
        }
        param.setContext(context);
        CommonId tableId = param.getTableId();
        CommonId txnId = vertex.getTask().getTxnId();
        CommonId partId = context.getDistribution().getId();
        DingoType schema = param.getSchema();
        StoreInstance localStore = Services.LOCAL_STORE.getInstance(tableId, partId);
        KeyValueCodec codec = param.getCodec();
        boolean isVector = false;
        boolean isDocument = false;
        if (context.getIndexId() != null) {
            Table indexTable = (Table) TransactionManager.getIndex(txnId, context.getIndexId());
            if (indexTable == null) {
                LogUtils.error(log, "[ddl] TxnPartInsert get index table null, indexId:{}", context.getIndexId());
                return false;
            }
            if (!OpStateUtils.allowWrite(indexTable.getSchemaState())) {
                return true;
            }
            List<Integer> columnIndices = param.getTable().getColumnIndices(indexTable.columns.stream()
                .map(Column::getName)
                .collect(Collectors.toList()));
            tableId = context.getIndexId();
            if (!param.isPessimisticTxn()) {
                Object defaultVal = null;
                if (columnIndices.contains(-1)) {
                    Column addColumn = indexTable.getColumns().stream()
                        .filter(column -> column.getSchemaState() != SchemaState.SCHEMA_PUBLIC)
                        .findFirst().orElse(null);
                    if (addColumn != null) {
                        defaultVal = addColumn.getDefaultVal();
                    }
                }
                Object[] finalTuple = tuple;
                Object finalDefaultVal = defaultVal;
                tuple = columnIndices.stream().map(i -> {
                    if (i == -1) {
                        return finalDefaultVal;
                    }
                    return finalTuple[i];
                }).toArray();
            }
            IndexTable index = (IndexTable) TransactionManager.getIndex(txnId, tableId);
            if (index.indexType.isVector) {
                isVector = true;
            }
            if (index.indexType == IndexType.DOCUMENT) {
                isDocument = true;
            }
            schema = indexTable.tupleType();
            localStore = Services.LOCAL_STORE.getInstance(context.getIndexId(), partId);
            codec = CodecService.getDefault().createKeyValueCodec(
                indexTable.getCodecVersion(), indexTable.version, indexTable.tupleType(), indexTable.keyMapping()
            );
        }
        Object[] newTuple = (Object[]) schema.convertFrom(tuple, ValueConverter.INSTANCE);
        KeyValue keyValue = wrap(codec::encode).apply(newTuple);
        CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
        byte[] key = keyValue.getKey();
        byte[] txnIdByte = txnId.encode();
        byte[] tableIdByte = tableId.encode();
        byte[] partIdByte = partId.encode();
        byte[] jobIdByte = vertex.getTask().getJobId().encode();
        int len = txnIdByte.length + tableIdByte.length + partIdByte.length;
        if (param.isPessimisticTxn()) {
            byte[] keyValueKey = keyValue.getKey();
            // dataKeyValue   [10_txnId_tableId_partId_a_putIf, value]
            byte[] dataKey = ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_DATA,
                keyValueKey,
                Op.PUTIFABSENT.getCode(),
                len,
                txnIdByte, tableIdByte, partIdByte);
            byte[] deleteKey = Arrays.copyOf(dataKey, dataKey.length);
            deleteKey[deleteKey.length - 2] = (byte) Op.DELETE.getCode();
            byte[] updateKey = Arrays.copyOf(dataKey, dataKey.length);
            updateKey[updateKey.length - 2] = (byte) Op.PUT.getCode();
            List<byte[]> bytes = new ArrayList<>(3);
            bytes.add(dataKey);
            bytes.add(deleteKey);
            bytes.add(updateKey);
            List<KeyValue> keyValues = localStore.get(bytes);
            if (keyValues != null && !keyValues.isEmpty()) {
                if (keyValues.size() > 1) {
                    throw new RuntimeException(txnId + " PrimaryKey is not existed than two in local store");
                }
                KeyValue value = keyValues.get(0);
                byte[] oldKey = value.getKey();
                if (oldKey[oldKey.length - 2] == Op.PUTIFABSENT.getCode()
                    || oldKey[oldKey.length - 2] == Op.PUT.getCode()) {
                    if (context.isDuplicateKey()) {
                        // insert into ... on duplicate key update ...
                        KeyValue insertUpKv = generateNewKv(
                            tuple,
                            param,
                            partId,
                            codec,
                            newTuple,
                            txnIdByte,
                            tableIdByte,
                            partIdByte,
                            len);
                        byte[] extraKey = ByteUtils.encode(
                            CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                            key,
                            oldKey[oldKey.length - 2],
                            len,
                            jobIdByte,
                            tableIdByte,
                            partIdByte
                        );
                        KeyValue extraKeyValue;
                        if (value.getValue() == null) {
                            // delete
                            extraKeyValue = new KeyValue(extraKey, null);
                        } else {
                            extraKeyValue = new KeyValue(
                                extraKey, Arrays.copyOf(value.getValue(), value.getValue().length)
                            );
                        }
                        localStore.put(extraKeyValue);
                        localStore.delete(deleteKey);
                        vertex.getTask().getPartData().put(
                            new TxnPartData(tableId, partId),
                            (!isVector && !isDocument)
                        );
                        if (localStore.put(insertUpKv) && context.getIndexId() == null) {
                            param.inc();
                        }
                    } else {
                        throw new DuplicateEntryException("Duplicate entry "
                            + TransactionUtil.duplicateEntryKey(tableId, key, txnId) + " for key 'PRIMARY'");
                    }
                } else {
                    KeyValue insertUpKv = null;
                    if (context.isDuplicateKey()) {
                        insertUpKv = generateNewKv(
                            tuple,
                            param,
                            partId,
                            codec,
                            newTuple,
                            txnIdByte,
                            tableIdByte,
                            partIdByte,
                            len);
                    }
                    // extraKeyValue  [12_jobId_tableId_partId_a_none, oldValue]
                    byte[] extraKey = ByteUtils.encode(
                        CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                        key,
                        oldKey[oldKey.length - 2],
                        len,
                        jobIdByte,
                        tableIdByte,
                        partIdByte
                    );
                    KeyValue extraKeyValue;
                    if (value.getValue() == null) {
                        // delete
                        extraKeyValue = new KeyValue(extraKey, null);
                    } else {
                        extraKeyValue = new KeyValue(
                            extraKey, Arrays.copyOf(value.getValue(), value.getValue().length)
                        );
                    }
                    localStore.put(extraKeyValue);
                    // delete  ->  insert  convert --> put
                    dataKey[dataKey.length - 2] = (byte) Op.PUT.getCode();
                    // write data
                    if (insertUpKv != null && insertUpKv.getValue() != null) {
                        keyValue.setKey(insertUpKv.getKey());
                        keyValue.setValue(insertUpKv.getValue());
                    } else {
                        keyValue.setKey(dataKey);
                    }
                    localStore.delete(deleteKey);
                    vertex.getTask().getPartData().put(
                        new TxnPartData(tableId, partId),
                        (!isVector && !isDocument)
                    );
                    if (localStore.put(keyValue) && context.getIndexId() == null) {
                        param.inc();
                    }
                }
            } else {
                KeyValue insertUpKv = null;
                if (context.isDuplicateKey()) {
                    insertUpKv = generateNewKv(
                        tuple,
                        param,
                        partId,
                        codec,
                        newTuple,
                        txnIdByte,
                        tableIdByte,
                        partIdByte,
                        len);
                }
                byte[] rollBackKey = ByteUtils.getKeyByOp(
                    CommonId.CommonType.TXN_CACHE_RESIDUAL_LOCK, Op.DELETE, dataKey
                );
                if (localStore.get(rollBackKey) != null) {
                    localStore.delete(rollBackKey);
                }
                // extraKeyValue  [12_jobId_tableId_partId_a_none, oldValue]
                byte[] extraKey = ByteUtils.encode(
                    CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                    key,
                    Op.NONE.getCode(),
                    len,
                    jobIdByte,
                    tableIdByte,
                    partIdByte
                );
                localStore.put(new KeyValue(extraKey, Arrays.copyOf(keyValue.getValue(), keyValue.getValue().length)));
                // write data
                if (insertUpKv != null && insertUpKv.getValue() != null) {
                    keyValue.setKey(insertUpKv.getKey());
                    keyValue.setValue(insertUpKv.getValue());
                } else {
                    keyValue.setKey(dataKey);
                }
                vertex.getTask().getPartData().put(
                    new TxnPartData(tableId, partId),
                    (!isVector && !isDocument)
                );
                if (localStore.put(keyValue) && context.getIndexId() == null) {
                    param.inc();
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
            Op op = Op.NONE;
            KeyValue insertUpKv = null;
            if (param.getUpdateMapping() != null && param.getUpdates() != null) {
                StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, partId);
                KeyValue oldKv = kvStore.txnGet(txnId.seq, key, param.getLockTimeOut());
                if (oldKv != null && oldKv.getValue() != null) {
                    context.setDuplicateKey(true);
                }
            }
            if (keyValues != null && !keyValues.isEmpty()) {
                if (keyValues.size() > 1) {
                    throw new RuntimeException(txnId + " Key is not existed than two in local store");
                }
                KeyValue value = keyValues.get(0);
                byte[] oldKey = value.getKey();
                if (oldKey[oldKey.length - 2] == Op.PUTIFABSENT.getCode()
                    || oldKey[oldKey.length - 2] == Op.PUT.getCode()) {
                    if (param.getUpdateMapping() != null && param.getUpdates() != null) {
                        insertUpKv = generateNewKv(
                            tuple,
                            param,
                            partId,
                            codec,
                            newTuple,
                            txnIdByte,
                            tableIdByte,
                            partIdByte,
                            len);
                        op = Op.PUT;
                    } else {
                        throw new DuplicateEntryException("Duplicate entry "
                            + TransactionUtil.duplicateEntryKey(tableId, key, txnId) + " for key 'PRIMARY'");
                    }
                } else {
                    // delete  ->  insert  convert --> put
                    insertKey[updateKey.length - 2] = (byte) Op.PUT.getCode();
                    op = Op.DELETE;
                }
            } else {
                if (!context.isDuplicateKey() && param.isCheckInPlace()) {
                    byte[] originalKey;
                    if (isVector) {
                        originalKey = codec.encodeKeyPrefix(newTuple, 1);
                        CodecService.getDefault().setId(originalKey, partId.domain);
                    } else if (isDocument) {
                        originalKey = codec.encodeKeyPrefix(newTuple, 1);
                        CodecService.getDefault().setId(originalKey, partId.domain);
                    } else {
                        originalKey = key;
                    }
                    StoreInstance kvStore = Services.KV_STORE.getInstance(tableId, partId);
                    KeyValue kvKeyValue = kvStore.txnGet(
                        txnId.seq,
                        originalKey,
                        param.getLockTimeOut()
                    );
                    if (kvKeyValue != null && kvKeyValue.getValue() != null) {
                        throw new DuplicateEntryException("Duplicate entry " +
                            TransactionUtil.duplicateEntryKey(tableId, key, txnId) + " for key 'PRIMARY'");
                    }
                }
                if (context.isDuplicateKey()) {
                    insertUpKv = generateNewKv(
                        tuple,
                        param,
                        partId,
                        codec,
                        newTuple,
                        txnIdByte,
                        tableIdByte,
                        partIdByte,
                        len);
                } else {
                    keyValue.setKey(
                        ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_CHECK_DATA, Op.CheckNotExists, insertKey)
                    );
                    localStore.put(keyValue);
                }
            }
            if (insertUpKv != null && insertUpKv.getValue() != null) {
                keyValue.setKey(insertUpKv.getKey());
                keyValue.setValue(insertUpKv.getValue());
            } else {
                keyValue.setKey(insertKey);
            }
            localStore.delete(deleteKey);
            // extraKeyValue  [12_jobId_tableId_partId_a_none, oldValue]
            byte[] extraKey = ByteUtils.encode(
                CommonId.CommonType.TXN_CACHE_EXTRA_DATA,
                key,
                op.getCode(),
                len,
                jobIdByte,
                tableIdByte,
                partIdByte
            );
            vertex.getTask().getPartData().put(
                new TxnPartData(tableId, partId),
                (!isVector && !isDocument)
            );
            localStore.put(new KeyValue(extraKey, Arrays.copyOf(keyValue.getValue(), keyValue.getValue().length)));
            if (localStore.put(keyValue) && context.getIndexId() == null) {
                param.inc();
                context.addKeyState(true);
            }
        }
        profile.time(start - System.currentTimeMillis());
        return true;
    }

    private static KeyValue generateNewKv(Object[] tuple,
                                          TxnPartInsertParam param,
                                          CommonId partId,
                                          KeyValueCodec codec,
                                          Object[] newTuple,
                                          byte[] txnIdByte,
                                          byte[] tableIdByte,
                                          byte[] partIdByte,
                                          int len) {
        KeyValue insertUpKv;
        TupleMapping mapping = param.getUpdateMapping();
        List<SqlExpr> updates = param.getUpdates();
        for (int i = 0; i < mapping.size(); i++) {
            Object newValue = updates.get(i).eval(tuple);
            int index = mapping.get(i);
            if ((newTuple[index] == null && newValue != null)
                || (newTuple[index] != null && !newTuple[index].equals(newValue))) {
                newTuple[index] = newValue;
            }
        }
        KeyValue updateKv = wrap(codec::encode).apply(newTuple);
        CodecService.getDefault().setId(updateKv.getKey(), partId.domain);
        byte[] insertKey = ByteUtils.encode(
            CommonId.CommonType.TXN_CACHE_DATA,
            updateKv.getKey(),
            Op.PUT.getCode(),
            len,
            txnIdByte,
            tableIdByte,
            partIdByte
        );
        insertUpKv = new KeyValue(
            insertKey, Arrays.copyOf(updateKv.getValue(), updateKv.getValue().length));
        return insertUpKv;
    }


    @Override
    public void fin(int pin, Fin fin, Vertex vertex) {
        synchronized (vertex) {
            TxnPartInsertParam param = vertex.getParam();
            Edge edge = vertex.getSoleEdge();
            if (!(fin instanceof FinWithException)) {
                edge.transformToNext(new Object[]{param.getCount()});
            }
            if (fin instanceof FinWithProfiles) {
                FinWithProfiles finWithProfiles = (FinWithProfiles) fin;
                finWithProfiles.addProfile(vertex);
                Long autoIncId = !param.getAutoIncList().isEmpty() ? param.getAutoIncList().get(0) : null;
                if (autoIncId != null) {
                    finWithProfiles.getProfile().setAutoIncId(autoIncId);
                    finWithProfiles.getProfile().setHasAutoInc(true);
                    param.getAutoIncList().remove(0);
                }
            }
            edge.fin(fin);
            // Reset
            param.reset();
        }
    }
}
