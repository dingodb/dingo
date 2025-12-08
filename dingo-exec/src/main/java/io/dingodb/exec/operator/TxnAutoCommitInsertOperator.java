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
import io.dingodb.common.exception.DingoTypeRangeException;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.util.Utils;
import io.dingodb.exec.Services;
import io.dingodb.exec.converter.ValueConverter;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.FinWithProfiles;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.TxnAutoCommitInsertParam;
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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public class TxnAutoCommitInsertOperator extends PartModifyOperator {
    public static final TxnAutoCommitInsertOperator INSTANCE = new TxnAutoCommitInsertOperator();

    private TxnAutoCommitInsertOperator() {
    }

    @Override
    protected boolean pushTuple(Context context, Object[] tuple, Vertex vertex) {
        TxnAutoCommitInsertParam param = vertex.getParam();
        OperatorProfile profile = param.getProfile("partAutoCommitInsertLocal");
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
        Table indexTable;

        Utils.checkAndUpdateTuples(schema, tuple);

        //Only for origin table.
        if (context.getIndexId() == null) {
            List<Column> originColumns = ((Table) TransactionManager.getTable(txnId, tableId)).getColumns();
            for (int i = 0; i < originColumns.size(); i++) {
                DingoType t = originColumns.get(i).getType();
                if (t instanceof io.dingodb.common.type.scalar.DecimalType) {
                    if (tuple[i] instanceof BigDecimal) {
                        long valueIntPart = ((BigDecimal) tuple[i]).precision() - ((BigDecimal) tuple[i]).scale();
                        long typeIntPart = ((io.dingodb.common.type.scalar.DecimalType) t).getPrecision()
                            - ((io.dingodb.common.type.scalar.DecimalType) t).getScale();
                        if (valueIntPart > typeIntPart) {
                            throw new DingoTypeRangeException(0, "Out of range value for column '" + originColumns.get(i).getName() + "'");
                        }
                    }
                } else if (originColumns.get(i).getSqlTypeName().equalsIgnoreCase("TINYINT")) {
                    if (tuple[i] instanceof Integer) {
                        if (!Utils.tinyintInRange((Integer)tuple[i])) {
                            throw new DingoTypeRangeException(0, "Out of range value for column '" + originColumns.get(i).getName() + "'");
                        }
                    }
                }
            }
        }

        if (context.getIndexId() != null) {
            indexTable = (Table) TransactionManager.getIndex(txnId, context.getIndexId());
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
            if (((IndexTable)indexTable).indexType.isVector) {
                isVector = true;
            }
            if (((IndexTable)indexTable).indexType == IndexType.DOCUMENT) {
                isDocument = true;
            }
            schema = indexTable.tupleType();
            if (context.isWithoutPrimary()) {
                schema.setCheckFieldCount(false);
                DingoType dingoType = codec.getDingoType();
                if (dingoType != null) {
                    dingoType.setCheckFieldCount(false);
                }
            }
            localStore = Services.LOCAL_STORE.getInstance(context.getIndexId(), partId);
            codec = CodecService.getDefault().createKeyValueCodec(
                indexTable.getCodecVersion(), indexTable.version, indexTable.tupleType(), indexTable.keyMapping()
            );
        }
        if (context.isWithoutPrimary()) {
            schema.setCheckFieldCount(false);
            DingoType dingoType = codec.getDingoType();
            if (dingoType != null) {
                dingoType.setCheckFieldCount(false);
            }
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
        byte[] insertKey = ByteUtils.encode(
            CommonId.CommonType.TXN_CACHE_DATA,
            keyValue.getKey(),
            Op.PUTIFABSENT.getCode(),
            (txnIdByte.length + tableIdByte.length + partIdByte.length),
            txnIdByte,
            tableIdByte,
            partIdByte);
        KeyValue loaclKeyValue = localStore.get(insertKey);
        if (loaclKeyValue != null && loaclKeyValue.getKey() != null) {
            throw new DuplicateEntryException("Duplicate entry "
                + TransactionUtil.duplicateEntryKey(tableId, key, txnId) + " for key 'PRIMARY'");
        } else {
            if (param.isCheckInPlace()) {
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
            keyValue.setKey(
                ByteUtils.getKeyByOp(CommonId.CommonType.TXN_CACHE_CHECK_DATA, Op.CheckNotExists, insertKey)
            );
            localStore.put(keyValue);
        }
        keyValue.setKey(insertKey);
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
        vertex.getTask().getPartData().put(
            new TxnPartData(tableId, partId),
            (!isVector && !isDocument)
        );
        localStore.put(new KeyValue(extraKey, Arrays.copyOf(keyValue.getValue(), keyValue.getValue().length)));
        if (localStore.put(keyValue) && context.getIndexId() == null) {
            param.inc();
            context.addKeyState(true);
        }
        profile.time(start - System.currentTimeMillis());
        return true;
    }

    @Override
    public void fin(int pin, Fin fin, Vertex vertex) {
        synchronized (vertex) {
            TxnAutoCommitInsertParam param = vertex.getParam();
            Edge edge = vertex.getSoleEdge();
            if (fin instanceof FinWithException) {
                edge.fin(fin);
                param.reset();
                return;
            }
            edge.transformToNext(new Object[]{param.getCount()});
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
