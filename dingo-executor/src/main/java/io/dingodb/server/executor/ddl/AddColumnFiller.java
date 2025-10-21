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

package io.dingodb.server.executor.ddl;

import com.google.common.collect.Iterators;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.ddl.ReorgBackFillTask;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.meta.SchemaState;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.ListType;
import io.dingodb.common.type.MapType;
import io.dingodb.common.type.scalar.BinaryType;
import io.dingodb.common.type.scalar.BitType;
import io.dingodb.common.type.scalar.BooleanType;
import io.dingodb.common.type.scalar.DateType;
import io.dingodb.common.type.scalar.DecimalType;
import io.dingodb.common.type.scalar.DoubleType;
import io.dingodb.common.type.scalar.FloatType;
import io.dingodb.common.type.scalar.IntegerType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.type.scalar.ObjectType;
import io.dingodb.common.type.scalar.StringType;
import io.dingodb.common.type.scalar.TimeType;
import io.dingodb.common.type.scalar.TimestampType;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.ByteUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Utils;
import io.dingodb.exec.Services;
import io.dingodb.exec.transaction.base.CacheToObject;
import io.dingodb.exec.transaction.base.TxnLocalData;
import io.dingodb.exec.transaction.util.TransactionCacheToMutation;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import io.dingodb.meta.InfoSchemaService;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.exception.WriteConflictException;
import io.dingodb.tso.TsoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static io.dingodb.calcite.runtime.DingoResource.DINGO_RESOURCE;
import static io.dingodb.common.CommonId.CommonType.FILL_BACK;
import static io.dingodb.common.mysql.MysqlByteUtil.binaryPrefix;
import static io.dingodb.common.util.NoBreakFunctions.wrap;
import static io.dingodb.exec.transaction.util.TransactionUtil.max_pre_write_count;

@Slf4j
public class AddColumnFiller extends IndexAddFiller {
    private Object defaultVal = null;

    boolean withoutPrimary;

    private CommonId replicaId;
    private int addPos;

    public Object getFillerValue(Column newColumn) {
        DingoType type = newColumn.getType();
        if (newColumn.getDefaultValueExpr() == null) {
            if (!newColumn.isNullable()) {
                return newColumn.getInitVal();
            }
        } else {
            String defaultValueExpr = newColumn.defaultValueExpr;
            if (defaultValueExpr.startsWith("'") && defaultValueExpr.endsWith("'")) {
                defaultValueExpr = SqlParserUtil.trim(defaultValueExpr, "'");
            }
            if (type instanceof StringType) {
                return Utils.decodePostgresUnicode(defaultValueExpr);
            } else if (type instanceof LongType) {
                return Long.parseLong(defaultValueExpr);
            } else if (type instanceof IntegerType) {
                return Integer.parseInt(defaultValueExpr);
            } else if (type instanceof DoubleType) {
                return Double.parseDouble(defaultValueExpr);
            } else if (type instanceof FloatType) {
                return Float.parseFloat(defaultValueExpr);
            } else if (type instanceof DateType) {
                if ("current_date".equalsIgnoreCase(defaultValueExpr)) {
                    return new Date(System.currentTimeMillis());
                }
                return DateTimeUtils.parseDate(defaultValueExpr);
            } else if (type instanceof DecimalType) {
                return new BigDecimal(defaultValueExpr);
            } else if (type instanceof BooleanType) {
                if (defaultValueExpr.equalsIgnoreCase("true")) {
                    return true;
                } else if (defaultValueExpr.equalsIgnoreCase("false")) {
                    return false;
                } else if (defaultValueExpr.equalsIgnoreCase("1")) {
                    return true;
                } else {
                    return false;
                }
            } else if (type instanceof TimestampType) {
                if (defaultValueExpr.equalsIgnoreCase("current_timestamp")) {
                    return new Timestamp(System.currentTimeMillis());
                }
                return DateTimeUtils.parseTimestamp(defaultValueExpr);
            } else if (type instanceof TimeType) {
                return DateTimeUtils.parseTime(defaultValueExpr);
            } else if (type instanceof ListType) {
                if (defaultValueExpr.toUpperCase().startsWith("ARRAY[") && defaultValueExpr.endsWith("]")) {
                    defaultValueExpr = defaultValueExpr.substring(6, defaultValueExpr.length() - 1);
                }
                if ("{}".equalsIgnoreCase(defaultValueExpr)) {
                    return new ArrayList<>();
                }
                String elementTypeName = newColumn.elementTypeName;
                List<String> list = Arrays.asList(defaultValueExpr.split(","));
                return list.stream().map(item -> {
                    switch (elementTypeName) {
                        case "FLOAT":
                            return Float.parseFloat(item);
                        case "DOUBLE":
                            return Double.parseDouble(item);
                        case "INTEGER":
                            return Integer.parseInt(item);
                        case "LONG":
                            return Long.parseLong(item);
                        case "BOOLEAN":
                            return Boolean.parseBoolean(item);
                        case "DATE":
                            return DateTimeUtils.parseDate(item);
                        case "DECIMAL":
                            return new BigDecimal(item);
                        case "TIMESTAMP":
                            return DateTimeUtils.parseTimestamp(item);
                        case "TIME":
                            return DateTimeUtils.parseTime(item);
                        default:
                            return item;
                    }
                }).collect(Collectors.toList());
            } else if (type instanceof MapType || (type instanceof ObjectType)) {
                if (defaultValueExpr.toUpperCase().startsWith("MAP[") && defaultValueExpr.endsWith("]")) {
                    defaultValueExpr = defaultValueExpr.substring(4, defaultValueExpr.length() - 1);
                }
                if ("{}".equalsIgnoreCase(defaultValueExpr)) {
                    return new LinkedHashMap<>();
                }
                List<String> list = Arrays.asList(defaultValueExpr.split(","));
                LinkedHashMap<Object, Object> mapVal = new LinkedHashMap<>();
                for (int j = 0; j < list.size(); j += 2) {
                    String key = list.get(j).trim();
                    String val = list.get(j + 1).trim();
                    key = SqlParserUtil.trim(key, "'");
                    val = SqlParserUtil.trim(val, "'");
                    mapVal.put(key, val);
                }
                return mapVal;
            } else if (type instanceof BitType) {
                defaultValueExpr = defaultValueExpr.substring(2, defaultValueExpr.length() - 1);
                BigInteger bigInt = new BigInteger(defaultValueExpr, 16);
                String binaryString = bigInt.toString(2);
                return Long.parseLong(binaryString, 2);
            } else if (type instanceof BinaryType) {
                if (binaryPrefix(defaultValueExpr)) {
                    defaultValueExpr = defaultValueExpr.substring(2, defaultValueExpr.length() - 1);
                    return ByteUtils.hexStringToByteArray(defaultValueExpr);
                } else {
                    return defaultValueExpr.getBytes();
                }
            }
            return defaultValueExpr;
        }
        return null;
    }

    @Override
    public boolean preWritePrimary(ReorgBackFillTask task) {
        ownerRegionId = task.getRegionId().seq;
        txnId = new CommonId(CommonId.CommonType.TRANSACTION, 0, task.getStartTs());
        txnIdKey = txnId.encode();
        commitTs = TsoService.getDefault().tso();
        table = InfoSchemaService.root().getTableDef(task.getTableId().domain, task.getTableId().seq);
        withoutPrimary = table.getColumns().stream().anyMatch(column -> column.isPrimary() && column.getState() == 2);
        indexTable = InfoSchemaService.root().getIndexDef(task.getTableId().domain, task.getTableId().seq,
             task.getIndexId().seq);
        initFiller();
        Column addColumn = indexTable.getColumns().stream()
            .filter(column -> column.getSchemaState() == SchemaState.SCHEMA_WRITE_REORG)
            .findFirst().orElse(null);
        if (addColumn == null) {
            throw new RuntimeException("new column not found");
        }
        defaultVal = getFillerValue(addColumn);
        if (indexTable.getProperties() != null) {
            addPos = Integer.parseInt(indexTable.getProperties().getProperty("addPos"));
        }
        indexCodec = CodecService.getDefault()
            .createKeyValueCodec(indexTable.codecVersion, indexTable.version,
            indexTable.tupleType(), indexTable.keyMapping());
        ps = PartitionService.getService(
            Optional.ofNullable(indexTable.getPartitionStrategy())
                .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
        // reorging when region split
        StoreInstance kvStore = Services.KV_STORE.getInstance(task.getTableId(), task.getRegionId());
        KeyValueCodec codec  = CodecService.getDefault().createKeyValueCodec(
            table.getCodecVersion(), table.getVersion(), table.tupleType(), table.keyMapping()
        );
        Iterator<KeyValue> iterator = kvStore.txnScanWithoutStream(
            task.getStartTs(),
            new StoreInstance.Range(task.getStart(), task.getEnd(), task.isWithStart(), task.isWithEnd()),
            50000
        );
        tupleIterator = Iterators.transform(iterator,
            wrap(codec::decode)::apply
        );
        boolean preRes = false;
        while (tupleIterator.hasNext()) {
            Object[] tuples = tupleIterator.next();
            Object[] tuplesTmp = getNewTuples(colLen, tuples);

            KeyValue keyValue = wrap(indexCodec::encode).apply(tuplesTmp);
            if (keyValue == null) {
                throw DINGO_RESOURCE.invalidDefaultValue(addColumn.getName()).ex();
            }
            NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges =
                getRegionList();
            CommonId partId = ps.calcPartId(keyValue.getKey(), ranges);
            CodecService.getDefault().setId(keyValue.getKey(), partId.domain);

            CommonId tableId = indexTable.tableId;
            int op = Op.PUT.getCode();
            byte[] key = keyValue.getKey();
            byte[] value = keyValue.getValue();
            primaryObj = new CacheToObject(TransactionCacheToMutation.cacheToMutation(
                op, key, value,0L, tableId, partId, txnId), tableId, partId
            );
            try {
                preWritePrimaryKey(primaryObj);
            } catch (WriteConflictException e) {
                conflict.incrementAndGet();
                continue;
            }
            preRes = true;
            break;
        }
        return preRes;
    }

    @NonNull
    public Object[] getNewTuples(int colLen, Object[] tuples) {
        List<Object> valList = new ArrayList<>(tuples.length + 1);
        for (Object valItem : tuples) {
            valList.add(valItem);
        }
        valList.add(addPos, defaultVal);
        tuples = valList.toArray();
        return tuples;

        //Object[] tuplesTmp = new Object[colLen + 1];
        //for (int i = 0; i < colLen; i++) {
        //    tuplesTmp[i] = tuples[columnIndices.get(i)];
        //}
        //if (withoutPrimary) {
        //    tuplesTmp[colLen] = tuplesTmp[colLen - 1];
        //    tuplesTmp[colLen - 1] = defaultVal;
        //} else {
        //    tuplesTmp[colLen] = defaultVal;
        //}
        //return tuplesTmp;
    }

    @Override
    public BackFillResult backFillDataInTxn(ReorgBackFillTask task, boolean withCheck) {
        CommonId tableId = task.getTableId();
        Iterator<Object[]> tupleIterator;
        if (task.getRegionId().seq != ownerRegionId) {
            tupleIterator = getIterator(task, tableId, withCheck);
        } else {
            tupleIterator = this.tupleIterator;
        }
        long start = System.currentTimeMillis();
        Map<String, TxnLocalData> caches = new TreeMap<>();
        long scanCount = 0;
        while (tupleIterator.hasNext()) {
            scanCount += 1;
            if (scanCount % 409600 == 0) {
                LogUtils.info(log, "bckFillDataInTxn loop count:{}, regionId:{}", scanCount, task.getRegionId());
            }
            Object[] tuple = tupleIterator.next();
            Object[] tuplesTmp = getNewTuples(colLen, tuple);
            TxnLocalData txnLocalData = getTxnLocalData(tuplesTmp);
            if (indexTable.unique && ByteArrayUtils.compare(txnLocalData.getKey(), primaryKey, 1) == 0) {
                duplicateKey(tuplesTmp);
            }
            String cacheKey = Base64.getEncoder().encodeToString(txnLocalData.getKey());
            if (!caches.containsKey(cacheKey)) {
                caches.put(cacheKey, txnLocalData);
            } else if (indexTable.unique) {
                duplicateKey(tuplesTmp);
            }
            if (caches.size() % max_pre_write_count == 0) {
                try {
                    List<TxnLocalData> txnLocalDataList = new ArrayList<>(caches.values());
                    preWriteSecondSkipConflict(txnLocalDataList);
                } finally {
                    caches.clear();
                }
            }
        }

        this.scanCount.addAndGet(scanCount);
        BackFillResult backFillResult = BackFillResult.builder().scanCount(scanCount).build();
        Collection<TxnLocalData> tupleList = caches.values();
        if (tupleList.isEmpty()) {
            return backFillResult;
        }
        List<TxnLocalData> txnLocalDataList = new ArrayList<>(tupleList);
        preWriteSecondSkipConflict(txnLocalDataList);
        backFillResult.addCount(tupleList.size());
        LogUtils.info(log, "pre write second, regionId:{}, iterator cost:{}ms, scanCount:{}",
            task.getRegionId(), (System.currentTimeMillis() - start), scanCount);
        doneRegionIdList.add(task.getRegionId());
        return backFillResult;
    }

    @Override
    public BackFillResult backFillDataInTxnWithCheck(ReorgBackFillTask task, boolean withCheck) {
        CommonId tableId = task.getTableId();
        Iterator<Object[]> tupleIterator = getIterator(task, tableId, withCheck);
        StoreInstance cache = Services.LOCAL_STORE.getInstance(null, null);
        long start = System.currentTimeMillis();
        Map<String, TxnLocalData> caches = new TreeMap<>();
        long scanCount = 0;
        while (tupleIterator.hasNext()) {
            scanCount += 1;
            Object[] tuple = tupleIterator.next();
            Object[] tuplesTmp = getNewTuples(colLen, tuple);
            TxnLocalData txnLocalData = getTxnLocalData(tuplesTmp);
            if (indexTable.unique) {
                if (ByteArrayUtils.compare(txnLocalData.getKey(), primaryKey, 1) == 0) {
                    duplicateKey(tuplesTmp);
                } else {
                    byte[] key = getLocalKey(txnLocalData.getKey(), txnLocalData.getPartId().encode());
                    if (cache.get(key) != null) {
                        continue;
                    }
                }
            }
            String cacheKey = Base64.getEncoder().encodeToString(txnLocalData.getKey());
            if (!caches.containsKey(cacheKey)) {
                caches.put(cacheKey, txnLocalData);
            } else if (indexTable.unique) {
                duplicateKey(tuplesTmp);
            }
            if (caches.size() % max_pre_write_count == 0) {
                try {
                    List<TxnLocalData> txnLocalDataList = new ArrayList<>(caches.values());
                    preWriteSecondSkipConflict(txnLocalDataList);
                } finally {
                    caches.clear();
                }
            }
        }

        this.scanCount.addAndGet(scanCount);
        BackFillResult backFillResult = BackFillResult.builder().scanCount(scanCount).build();
        Collection<TxnLocalData> tupleList = caches.values();
        if (tupleList.isEmpty()) {
            return backFillResult;
        }
        List<TxnLocalData> txnLocalDataList = new ArrayList<>(tupleList);
        preWriteSecondSkipConflict(txnLocalDataList);
        backFillResult.addCount(tupleList.size());
        LogUtils.info(log, "pre write second with check, iterator cost:{}ms, scanCount:{}, regionId:{}",
            (System.currentTimeMillis() - start), scanCount, task.getRegionId());
        doneRegionIdList.add(task.getRegionId());
        return backFillResult;
    }

    @Override
    public TxnLocalData getTxnLocalData(Object[] tuples) {
        KeyValue keyValue = wrap(indexCodec::encode).apply(tuples);
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges =
            getRegionList();
        CommonId partId = ps.calcPartId(keyValue.getKey(), ranges);
        CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
        return TxnLocalData.builder()
            .dataType(FILL_BACK)
            .txnId(txnId)
            .tableId(indexTable.tableId)
            .partId(partId)
            .op(Op.PUT)
            .key(keyValue.getKey())
            .value(keyValue.getValue())
            .build();
    }

    @Override
    public NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> getRegionList() {
        return MetaService.root().getRangeDistribution(replicaId);
    }

    @Override
    public void initFiller() {
        super.initFiller();
        replicaId = indexTable.tableId;
        LogUtils.info(log, "replicaTableId:{}", replicaId);
    }

}
