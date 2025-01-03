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

package io.dingodb.calcite.executor;

import io.dingodb.calcite.DingoParserContext;
import io.dingodb.calcite.grammar.ddl.SqlLoadData;
import io.dingodb.calcite.runtime.DingoResource;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Utils;
import io.dingodb.exec.Services;
import io.dingodb.exec.converter.ImportFileConverter;
import io.dingodb.exec.transaction.base.TxnLocalData;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.transaction.util.Txn;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.exception.DuplicateEntryException;
import io.dingodb.store.api.transaction.exception.RegionSplitException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.dingodb.common.util.NoBreakFunctions.wrap;
import static io.dingodb.common.util.Utils.getByteIndexOf;
import static io.dingodb.exec.transaction.util.TransactionUtil.max_pre_write_count;

@Slf4j
public class LoadDataExecutor implements DmlExecutor {
    private final DingoParserContext context;

    private final String schemaName;

    private final String filePath;
    private final byte[] fieldsTerm;

    private final String enclosed;
    private final byte[] linesTerm;
    private final byte[] lineStarting;
    private final byte[] escaped;
    private String charset;
    private final int ignoreNum;

    private volatile boolean isDone;
    private volatile String errMessage;
    private final Table table;
    private final KeyValueCodec codec;
    private NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions;
    private final DingoType schema;

    MetaService metaService;
    private static int exceptionRetries = 0;
    private static final Long retryInterval = 6000L;
    private static final int maxRetries = 20;
    private int dataGenNum = 0;
    private final boolean isTxn;
    private final String statementId;
    private boolean txnRetry;
    private int txnRetryCnt;
    private long timeOut;
    private CompletableFuture<String> loadDataRead;
    private long start;

    private final AtomicLong count = new AtomicLong(0);

    private final BlockingQueue<Object> queue = new ArrayBlockingQueue<>(10000);

    public LoadDataExecutor(SqlLoadData sqlLoadData, Connection connection, DingoParserContext context) {
        this.context = context;
        this.filePath = sqlLoadData.getFilePath();
        this.fieldsTerm = sqlLoadData.getTerminated();
        this.enclosed = sqlLoadData.getEnclosed();
        this.linesTerm = sqlLoadData.getLineTerminated();
        this.escaped = sqlLoadData.getEscaped();
        if (sqlLoadData.getCharset() != null) {
            this.charset = sqlLoadData.getCharset();
        } else {
            try {
                this.charset = connection.getClientInfo("character_set_server");
            } catch (SQLException e) {
                this.charset = "utf8";
            }
        }
        try {
            String txnRetryStr = connection.getClientInfo("txn_retry");
            String retryCntStr = connection.getClientInfo("txn_retry_cnt");
            if (retryCntStr == null) {
                retryCntStr = "0";
            }
            txnRetry = "on".equalsIgnoreCase(txnRetryStr);
            txnRetryCnt = Integer.parseInt(retryCntStr);
            timeOut = 300000;
        } catch (SQLException e) {
            txnRetry = false;
            txnRetryCnt = 0;
        }

        this.schemaName = sqlLoadData.getSchemaName();
        this.lineStarting = sqlLoadData.getLineStarting();
        this.ignoreNum = sqlLoadData.getIgnoreNum();
        metaService = MetaService.root();
        //RootSnapshotSchema rootSnapshotSchema = (RootSnapshotSchema) context.getRootSchema().schema;
        InfoSchema is = DdlService.root().getIsLatest();
        table = is.getTable(schemaName, sqlLoadData.getTableName());
        if (table == null) {
            throw DingoResource.DINGO_RESOURCE.unknownTable(schemaName + "." + sqlLoadData.getTableName()).ex();
        }
        codec = CodecService.getDefault().createKeyValueCodec(
            table.getCodecVersion(), table.version, table.tupleType(), table.keyMapping()
        );
        distributions = metaService.getRangeDistribution(table.tableId);
        schema = table.tupleType();
        this.isTxn = checkEngine();
        this.statementId = UUID.randomUUID().toString();
    }

    @Override
    public boolean execute() {
        if (enclosed != null && enclosed.equals("()")) {
            throw DingoResource.DINGO_RESOURCE.fieldSeparatorError().ex();
        }
        start = System.currentTimeMillis();
        CompletableFuture<String> future = Executors.submit("loadDataDecoder", () -> {
            try {
                byte[] preBytes = null;
                long ver = DdlService.root().getIsLatest().getSchemaMetaVersion();
                context.getRootSchema().putRelatedTable(table.tableId.seq, ver);
                while (true) {
                    Object val = queue.take();
                    if (val instanceof byte[]) {
                        byte[] bytes = (byte[]) val;
                        preBytes = splitLine(bytes, preBytes, fieldsTerm, linesTerm);
                    } else {
                        break;
                    }
                }
                if (isTxn) {
                    endWriteWithTxn();
                }
                return null;
            } catch (DuplicateEntryException e1) {
                errMessage = "Duplicate entry for key 'PRIMARY'";
                return errMessage;
            } catch (Exception e2) {
                LogUtils.error(log, e2.getMessage(), e2);
                errMessage = e2.getMessage();
                return errMessage;
            } finally {
                context.getRootSchema().removeRelatedTable(table.tableId.seq);
                isDone = true;
            }
        });
        try {
            FileInputStream is = new FileInputStream(filePath);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            loadDataRead = Executors.submit("loadDataRead", () -> {
                try {
                    byte[] buffer = new byte[1000];
                    int length;
                    while ((length = is.read(buffer)) != -1) {
                        bos.write(buffer, 0, length);
                        queue.put(bos.toByteArray());
                        bos.reset();
                    }
                    queue.put("end");
                    bos.close();
                    is.close();
                    return null;
                } catch (IOException e) {
                    LogUtils.error(log, e.getMessage(), e);
                    return e.getMessage();
                }
            });
        } catch (FileNotFoundException e) {
            // Err code 2: No such file or directory
            throw DingoResource.DINGO_RESOURCE.accessError(filePath, 2, "No such file or directory").ex();
        } catch (Exception e) {
            throw DingoResource.DINGO_RESOURCE.loadDataError().ex();
        }
        future.whenCompleteAsync((r, e) -> {
            if (r != null) {
                if (!loadDataRead.isDone()) {
                    loadDataRead.cancel(true);
                }
            }
        });
        return true;
    }

    @Override
    public Iterator<Object[]> getIterator() {
        loadDataRead.whenComplete((r, e) -> {
            if (r != null) {
                throw DingoResource.DINGO_RESOURCE.accessError("filepath", 13, "Permission denied").ex();
            }
        });
        while (!isDone) {
            Utils.sleep(1000L);
        }
        long sub = System.currentTimeMillis() - start;
        long insertCount = count.get();
        LogUtils.info(log, "load data done, path:{}, cost:{}, insertCount:{}" , filePath, sub, insertCount);
        if (errMessage != null) {
            if (insertCount == 0) {
                if (errMessage.contains("Duplicate entry")) {
                    throw DingoResource.DINGO_RESOURCE.duplicateKey().ex();
                } else {
                    throw new RuntimeException(errMessage);
                }
            } else {
                List<SQLWarning> sqlWarningList = context.getWarningList();
                if (sqlWarningList == null) {
                    sqlWarningList = new ArrayList<>();
                }
                sqlWarningList.add(new SQLWarning(errMessage, errMessage, 1062));
                context.setWarningList(sqlWarningList);
            }
        }
        List<IndexTable> indexTableList = table.getIndexes();
        if (indexTableList != null) {
            insertCount = !indexTableList.isEmpty() ? insertCount / (indexTableList.size() + 1)  : insertCount;
        }
        List<Object[]> objects = new ArrayList<>();
        objects.add(new Object[] {insertCount});
        return objects.iterator();
    }

    @Override
    public String getWarning() {
        return errMessage;
    }

    // simple line split
    private byte[] splitLine(byte[] current, byte[] pre, byte[] fieldsTerm, byte[] linesTerm)
        throws UnsupportedEncodingException {
        byte[] bytes;
        if (pre != null) {
            bytes = new byte[current.length + pre.length];
            System.arraycopy(pre, 0, bytes, 0, pre.length);
            System.arraycopy(current, 0, bytes, pre.length, current.length);
        } else {
            bytes = current;
        }

        int len = bytes.length;
        int lineBreakPos = 0;
        int searchPos = 0;
        boolean isContinue = true;
        int loopCount = 0;
        while (isContinue) {
            searchPos = Math.max(searchPos, lineBreakPos);
            int id1 = getByteIndexOf(bytes, linesTerm, searchPos, len);
            if (id1 > 0) {
                byte[] lineBytes = new byte[id1 - lineBreakPos];
                System.arraycopy(bytes, lineBreakPos, lineBytes, 0, lineBytes.length);
                int id2 = getByteIndexOf(lineBytes, lineStarting, 0, lineBytes.length);
                if (id2 == 0 && bytes[id1 - 1] != escaped[0]) {
                    Object[] tuple = splitRow(lineBytes, fieldsTerm);
                    insertTuples(tuple);
                    int tmp1 = id1 + linesTerm.length;
                    if (tmp1 == len) {
                        isContinue = false;
                        lineBreakPos = tmp1;
                    }
                    if (tmp1 <= len - 1) {
                        lineBreakPos = tmp1;
                    }
                } else {
                    searchPos = id1 + 1;
                }
            } else {
                isContinue = false;
            }
            loopCount ++;
            if (loopCount >= len) {
                isContinue = false;
            }
        }
        byte[] preBytes = null;
        if (lineBreakPos <= len - 1) {
            preBytes = new byte[len - lineBreakPos];
            System.arraycopy(bytes, lineBreakPos, preBytes, 0, preBytes.length);
        }
        return preBytes;
    }

    private void insertTuples(Object[] tuples) {
        dataGenNum ++;
        // ignore rows
        if (dataGenNum <= ignoreNum) {
            return;
        }
        tuples = enclosed(tuples);
        tuples = processHideCol(tuples);
        tuples = (Object[]) schema.convertFrom(tuples, new ImportFileConverter(escaped));

        if (isTxn) {
            if (dataGenNum % max_pre_write_count == 0) {
                refreshTxnId = true;
            }
            insertWithTxn(tuples);
        } else {
            insertWithoutTxn(tuples, false);
        }
    }

    public void insertWithoutTxn(Object[] tuples, boolean retry) {
        try {
            if (retry) {
                distributions = metaService.getRangeDistribution(table.tableId);
            }
            CommonId partId = PartitionService.getService(
                    Optional.ofNullable(table.getPartitionStrategy())
                        .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
                .calcPartId(tuples, wrap(codec::encodeKey), distributions);
            StoreInstance store = Services.KV_STORE.getInstance(table.getTableId(), partId);
            boolean insert = store.insertIndex(tuples);
            if (insert) {
                insert = store.insertWithIndex(tuples);
            }
            if (insert) {
                count.incrementAndGet();
            }
            exceptionRetries = 0;
        } catch (Exception e) {
            LogUtils.error(log, e.getMessage(), e);
            if (e.getMessage().contains("epoch is not match, region_epoch")
                || e.getMessage().contains("Key out of range")
                || e instanceof RegionSplitException) {
                if (!continueRetry()) {
                    throw e;
                }
                insertWithoutTxn(tuples, true);
            } else {
                throw e;
            }
        }
    }

    public CommonId getTxnId() {
        if (refreshTxnId || txnId == null) {
            txnId = new CommonId(CommonId.CommonType.TRANSACTION,
                TransactionManager.getServerId().seq, TransactionManager.getStartTs());
        }
        return txnId;
    }

    CommonId txnId;
    boolean refreshTxnId = false;

    public void insertWithTxn(Object[] tuples) {
        ExecutionEnvironment env = ExecutionEnvironment.INSTANCE;
        Map<String, KeyValue> caches = env.memCacheFor2PC.memoryCache
            .computeIfAbsent(statementId, e -> new TreeMap<>());
        KeyValue keyValue = codec.encode(tuples);

        CommonId txnId = getTxnId();
        recodePriTable(keyValue, txnId);
        String cacheKey = Base64.getEncoder().encodeToString(keyValue.getKey());
        if (!caches.containsKey(cacheKey)) {
            caches.put(cacheKey, keyValue);
        }
        List<IndexTable> indexTableList = table.getIndexes();
        if (indexTableList != null) {
            for (IndexTable indexTable : indexTableList) {
                List<Integer> columnIndices = table.getColumnIndices(indexTable.columns.stream()
                    .map(Column::getName)
                    .collect(Collectors.toList()));
                Object[] tuplesTmp = columnIndices.stream().map(i -> tuples[i]).toArray();
                KeyValueCodec codec = CodecService.getDefault()
                    .createKeyValueCodec(
                        indexTable.getCodecVersion(), indexTable.version,
                        indexTable.tupleType(), indexTable.keyMapping());

                keyValue = wrap(codec::encode).apply(tuplesTmp);
                PartitionService ps = PartitionService.getService(
                    Optional.ofNullable(indexTable.getPartitionStrategy())
                        .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
                NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges =
                    metaService.getRangeDistribution(indexTable.tableId);
                CommonId partId = ps.calcPartId(keyValue.getKey(), ranges);
                CodecService.getDefault().setId(keyValue.getKey(), partId.domain);

                byte[] txnIdByte = txnId.encode();
                byte[] tableIdByte = indexTable.tableId.encode();
                byte[] partIdByte = partId.encode();
                keyValue.setKey(
                    ByteUtils.encode(
                        CommonId.CommonType.TXN_CACHE_DATA,
                        keyValue.getKey(),
                        Op.PUTIFABSENT.getCode(),
                        (txnIdByte.length + tableIdByte.length + partIdByte.length),
                        txnIdByte,
                        tableIdByte,
                        partIdByte)
                );
                cacheKey = Base64.getEncoder().encodeToString(keyValue.getKey());
                if (!caches.containsKey(cacheKey)) {
                    caches.put(cacheKey, keyValue);
                }
            }
        }

        if (refreshTxnId) {
            long start = System.currentTimeMillis();
            int cacheSize;
            Txn txn = new Txn(
                txnId, txnRetry, txnRetryCnt, timeOut
            );
            try {
                List<TxnLocalData> tupleList = getCacheTupleList(caches, txnId);
                int result = txn.commit(tupleList);
                count.addAndGet(result);
                cacheSize = caches.size();
                caches.clear();
            } finally {
                txn.close();
                env.memCacheFor2PC.memoryCache.remove(statementId);
            }
            long sub = System.currentTimeMillis() - start;
            long totalCount = count.get();
            if (totalCount % 409600 == 0) {
                LogUtils.info(log, "insert txn batch size: {}, cost time: {}ms, insert count:{}",
                    cacheSize, sub, totalCount);
            }
            refreshTxnId = false;
        }
    }

    private void recodePriTable(KeyValue keyValue, CommonId txnId) {
        CommonId partId = PartitionService.getService(
                Optional.ofNullable(table.getPartitionStrategy())
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
            .calcPartId(keyValue.getKey(), distributions);
        CodecService.getDefault().setId(keyValue.getKey(), partId.domain);
        byte[] txnIdByte = txnId.encode();
        byte[] tableIdByte = table.getTableId().encode();
        byte[] partIdByte = partId.encode();

        keyValue.setKey(ByteUtils.encode(
            CommonId.CommonType.TXN_CACHE_DATA,
            keyValue.getKey(),
            Op.PUTIFABSENT.getCode(),
            (txnIdByte.length + tableIdByte.length + partIdByte.length),
            txnIdByte, tableIdByte, partIdByte));
    }

    public void endWriteWithTxn() {
        long start = System.currentTimeMillis();
        ExecutionEnvironment env = ExecutionEnvironment.INSTANCE;
        try {
            CommonId txnId = getTxnId();
            Txn txnImportDataOperation = new Txn(
                txnId, txnRetry, txnRetryCnt, timeOut
            );
            Map<String, KeyValue> caches = env.memCacheFor2PC.memoryCache
                .computeIfAbsent(statementId, e -> new TreeMap<>());
            List<TxnLocalData> tupleList = getCacheTupleList(caches, txnId);
            if (tupleList.isEmpty()) {
                return;
            }
            int result = txnImportDataOperation.commit(tupleList);
            count.addAndGet(result);
            caches.clear();
        } finally {
            env.memCacheFor2PC.memoryCache.remove(statementId);
        }
        long end = System.currentTimeMillis();
        LogUtils.debug(log, "insert txn end batch, cost time:" + (end - start) + "ms");
    }

    public static List<TxnLocalData> getCacheTupleList(Map<String, KeyValue> keyValueMap, CommonId txnId) {
        List<TxnLocalData> tupleCacheList = new ArrayList<>();
        for (KeyValue keyValue : keyValueMap.values()) {
            TxnLocalData txnLocalData = getCacheTuples(keyValue);
            if (txnLocalData != null) {
                tupleCacheList.add(txnLocalData);
            }
        }
        return tupleCacheList;
    }

    public static TxnLocalData getCacheTuples(KeyValue keyValue) {
        Object[] caches = io.dingodb.exec.utils.ByteUtils.decode(keyValue);
        return (TxnLocalData) caches[0];
    }

    private static boolean continueRetry() {
        if (exceptionRetries > maxRetries) {
            return false;
        }
        Utils.sleep(retryInterval);
        exceptionRetries ++;
        return true;
    }

    public Object[] splitRow(byte[] bytes, byte[] terminated) throws UnsupportedEncodingException {
        if (lineStarting != null) {
            byte[] bytesTmp = new byte[bytes.length - lineStarting.length];
            System.arraycopy(bytes, lineStarting.length, bytesTmp, 0, bytesTmp.length);
            bytes = bytesTmp;
        }
        int len = bytes.length;
        int fieldBreakPos = 0;
        byte fieldsTermByte;
        boolean terminatedOnlyByte = false;
        int termLen = terminated.length;
        if (termLen == 1) {
            fieldsTermByte = terminated[0];
            terminatedOnlyByte = true;
        } else {
            fieldsTermByte = terminated[terminated.length - 1];
        }
        List<String> tupleList = new ArrayList<>();
        for (int i = 0; i < len; i ++) {
            byte b = bytes[i];
            if (terminatedOnlyByte && b == fieldsTermByte && i >= 1 && bytes[i - 1] != escaped[0]) {
                byte[] fieldBytes = new byte[i - fieldBreakPos];
                System.arraycopy(bytes, fieldBreakPos, fieldBytes, 0, fieldBytes.length);
                String valTmp = new String(fieldBytes, charset);
                if ("\\N".equalsIgnoreCase(valTmp)) {
                    tupleList.add(valTmp);
                } else {
                    tupleList.add(StringEscapeUtils.unescapeJson(valTmp));
                }
                fieldBreakPos = i + 1;
            } else if (!terminatedOnlyByte && b == fieldsTermByte) {
                // example fields term len == 5 and b == 24
                // bytes[23] == term[3] and bytes[22] == term[2] and bytes[21] == term[1] and bytes[20] == term[0]
                // bytes[19] != escaped[0]
                int ix = 1;
                boolean res3 = true;
                while (ix < termLen) {
                    boolean res = bytes[i - ix] == terminated[termLen - ix - 1];
                    if (!res) {
                        res3 = false;
                        break;
                    }
                    ix ++;
                }
                if (res3 && bytes[i - termLen] != escaped[0]) {
                    byte[] fieldBytes = new byte[i - termLen + 1 - fieldBreakPos];
                    System.arraycopy(bytes, fieldBreakPos, fieldBytes, 0, fieldBytes.length);
                    String valTmp = new String(fieldBytes, charset);
                    if ("\\N".equalsIgnoreCase(valTmp)) {
                        tupleList.add(valTmp);
                    } else {
                        tupleList.add(StringEscapeUtils.unescapeJson(valTmp));
                    }
                    fieldBreakPos = i + 1;
                }
            }
        }
        if (fieldBreakPos <= len - 1) {
            byte[] fieldBytes = new byte[len - fieldBreakPos];
            System.arraycopy(bytes, fieldBreakPos, fieldBytes, 0, fieldBytes.length);
            String valTmp = new String(fieldBytes, charset);
            if ("\\N".equalsIgnoreCase(valTmp)) {
                tupleList.add(valTmp);
            } else {
                tupleList.add(StringEscapeUtils.unescapeJson(valTmp));
            }
        } else if (bytes[len - 1] == fieldsTermByte) {
            if (terminatedOnlyByte) {
                // 1,2,3,
                // tuples 1, 2, 3, ""
                tupleList.add("");
            } else {
                // 1--2--3--
                // example i == 8 and termlen == 2
                // bytes[7] == termlen[0] && bytes[6] != escaped[0]
                int ix = 1;
                boolean res3 = true;
                while (ix < termLen) {
                    boolean res = bytes[len - ix - 1] == fieldsTerm[termLen - ix - 1];
                    if (!res) {
                        res3 = false;
                        break;
                    }
                    ix ++;
                }

                if (res3 && bytes[len - termLen - 1] != escaped[0]) {
                    tupleList.add("");
                }
            }
        }

        return tupleList.toArray(new String[0]);
    }

    private Object[] enclosed(Object[] tuples) {
        if (StringUtils.isBlank(enclosed)) {
            return tuples;
        }
        Object[] newTuples = new Object[tuples.length];
        for (int i = 0; i < tuples.length; i ++) {
            String item = tuples[i].toString();
            if (item.startsWith(enclosed) && item.endsWith(enclosed)) {
                newTuples[i] = enclosed(item, enclosed);
            } else {
                newTuples[i] = item;
            }
        }
        return newTuples;
    }

    public static String enclosed(String str, String enclosed) {
        StringBuilder patternBuilder = new StringBuilder("(^\\");
        patternBuilder.append(enclosed);
        patternBuilder.append("|");
        patternBuilder.append("\\");
        patternBuilder.append(enclosed);
        patternBuilder.append("$)");
        String pattern = patternBuilder.toString();
        return str.replaceAll(pattern, "");
    }

    private Object[] processHideCol(Object[] tuples) {
        boolean hasHide = false;
        boolean hasInc = false;
        int incColIdx = -1;
        for (Column colDef : table.getColumns()) {
            if (colDef.getState() == 2 && colDef.isAutoIncrement()) {
                hasHide = true;
                break;
            } else if (colDef.getState() == 1 && colDef.isAutoIncrement()) {
                incColIdx = table.getColumns().indexOf(colDef);
                hasInc = true;
                break;
            }
        }
        if (hasHide && schema.fieldCount() - tuples.length == 1) {
            Object[] tupleTmp = new Object[schema.fieldCount()];
            System.arraycopy(tuples, 0, tupleTmp, 0, tuples.length);
            Long id = metaService.getAutoIncrement(table.getTableId());
            tupleTmp[tuples.length] = id.toString();
            return tupleTmp;
        } else if (hasInc && schema.fieldCount() == tuples.length && incColIdx >= 0 && incColIdx < tuples.length) {
            MetaService metaService = MetaService.root();
            long autoIncVal = Long.parseLong(tuples[incColIdx].toString());
            metaService.updateAutoIncrement(table.getTableId(), autoIncVal);
        }
        return tuples;
    }

    private boolean checkEngine() {
        String engine = table.getEngine().toUpperCase();
        return StringUtils.isNotBlank(engine) && engine.contains("TXN");
    }

}
