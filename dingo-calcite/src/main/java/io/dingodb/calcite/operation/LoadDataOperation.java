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

import io.dingodb.calcite.DingoParserContext;
import io.dingodb.calcite.grammar.ddl.SqlLoadData;
import io.dingodb.calcite.runtime.DingoResource;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.Services;
import io.dingodb.exec.converter.ImportFileConverter;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.transaction.api.LockType;
import io.dingodb.transaction.api.TransactionService;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import io.dingodb.store.api.transaction.exception.DuplicateEntryException;
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
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public class LoadDataOperation implements DmlOperation {
    private DingoParserContext context;

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
    private Connection connection;

    private final AtomicLong count = new AtomicLong(0);

    private final BlockingQueue<Object> queue = new ArrayBlockingQueue<>(10000);

    public LoadDataOperation(SqlLoadData sqlLoadData, Connection connection, DingoParserContext context) {
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
            String timeOutStr = connection.getClientInfo("statement_timeout");
            if (timeOutStr == null) {
                timeOutStr = "50000";
            }
            txnRetry = "on".equalsIgnoreCase(txnRetryStr);
            txnRetryCnt = Integer.parseInt(retryCntStr);
            timeOut = Integer.parseInt(timeOutStr);
        } catch (SQLException e) {
            txnRetry = false;
            txnRetryCnt = 0;
        }

        String schemaName = sqlLoadData.getSchemaName();
        this.lineStarting = sqlLoadData.getLineStarting();
        this.ignoreNum = sqlLoadData.getIgnoreNum();
        metaService = MetaService.root().getSubMetaService(schemaName);
        table = metaService.getTable(sqlLoadData.getTableName());
        codec = CodecService.getDefault().createKeyValueCodec(table.tupleType(), table.keyMapping());
        distributions = metaService.getRangeDistribution(table.tableId);
        schema = table.tupleType();
        this.isTxn = checkEngine();
        this.statementId = UUID.randomUUID().toString();
        this.connection = connection;
    }

    @Override
    public boolean execute() {
        if (enclosed != null && enclosed.equals("()")) {
            throw DingoResource.DINGO_RESOURCE.fieldSeparatorError().ex();
        }
        if ((linesTerm.length >= 2 && !(linesTerm[0] == 0x0d && linesTerm[1] == 0x0a))
            || fieldsTerm.length >= 2
            || escaped.length >= 2) {
            throw DingoResource.DINGO_RESOURCE.fieldSeparatorError().ex();
        }
        try {
            new Thread(() -> {
                try {
                    byte[] preBytes = null;
                    List<CommonId> tables = new ArrayList<>();
                    tables.add(table.getTableId());
                    TransactionService.getDefault().lockTable(connection, tables, LockType.TABLE);
                    while (true) {
                        Object val = queue.take();
                        if (val instanceof byte[]) {
                            byte[] bytes = (byte[]) val;
                            preBytes = handMessage(bytes, preBytes, fieldsTerm, linesTerm);
                        } else {
                            break;
                        }
                    }
                    if (isTxn) {
                        endWriteWithTxn();
                    }
                } catch (DuplicateEntryException e1) {
                    errMessage = "Duplicate entry for key 'PRIMARY'";
                } catch (Exception e2) {
                    log.error(e2.getMessage(), e2);
                    errMessage = e2.getMessage();
                } finally {
                    TransactionService.getDefault().unlockTable(connection);
                    isDone = true;
                }
            }).start();
            FileInputStream is = new FileInputStream(filePath);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
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
        } catch (FileNotFoundException e) {
            // Err code 2: No such file or directory
            throw DingoResource.DINGO_RESOURCE.accessError(filePath, 2, "No such file or directory").ex();
        } catch (IOException e) {
            // Err code 13: Permission denied
            throw DingoResource.DINGO_RESOURCE.accessError("filepath", 13, "Permission denied").ex();
        } catch (Exception e) {
            throw DingoResource.DINGO_RESOURCE.loadDataError().ex();
        }
        return true;
    }

    @Override
    public Iterator<Object[]> getIterator() {
        while (!isDone) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        long insertCount = count.get();
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
        List<Object[]> objects = new ArrayList<>();
        objects.add(new Object[] {insertCount});
        return objects.iterator();
    }

    @Override
    public String getWarning() {
        return errMessage;
    }

    private byte[] handMessage(byte[] bytes, byte[] preBytes, byte[] fieldsTerm, byte[] linesTerm)
        throws UnsupportedEncodingException {
        boolean defaultLinesTerm = linesTerm.length == 1 && linesTerm[0] == 0x0a;
        int len = bytes.length;
        int lineBreakPos = 0;
        for (int i = 0; i < len; i ++) {
            byte b = bytes[i];
            // line break is hex 0d0a
            if ((linesTerm.length == 2 && b == linesTerm[1] && i >= 1 && bytes[i - 1] == linesTerm[0])
                || (defaultLinesTerm && b == 0x0a && i >= 1 && bytes[i - 1] == 0x0d)
                || (defaultLinesTerm && b == 0x0a && preBytes != null && preBytes[preBytes.length - 1] == 0x0d)) {
                byte[] lineBytes = new byte[i - lineBreakPos - 1];
                System.arraycopy(bytes, lineBreakPos, lineBytes, 0, lineBytes.length);

                if (preBytes != null) {
                    byte[] realBytes = new byte[preBytes.length + lineBytes.length];
                    System.arraycopy(preBytes, 0, realBytes, 0, preBytes.length);
                    System.arraycopy(lineBytes, 0, realBytes, preBytes.length, lineBytes.length);
                    preBytes = null;
                    lineBytes = realBytes;
                }
                Object[] tuple;
                if (fieldsTerm.length == 1) {
                    tuple = splitRow(lineBytes, fieldsTerm[0]);
                } else {
                    throw new RuntimeException("Fields terminated does not support multiple bytes");
                }
                insertTuples(tuple);
                lineBreakPos = i + 1;
            } else if ((linesTerm.length == 1 && b == linesTerm[0]) && ((i >= 1 && bytes[i - 1] != escaped[0])
                || (i == 0 && preBytes != null && preBytes[preBytes.length - 1] != escaped[0]))) {
                byte[] lineBytes = new byte[i - lineBreakPos];
                System.arraycopy(bytes, lineBreakPos, lineBytes, 0, lineBytes.length);

                if (preBytes != null) {
                    byte[] realBytes = new byte[preBytes.length + lineBytes.length];
                    System.arraycopy(preBytes, 0, realBytes, 0, preBytes.length);
                    System.arraycopy(lineBytes, 0, realBytes, preBytes.length, lineBytes.length);
                    preBytes = null;
                    lineBytes = realBytes;
                }
                Object[] tuple = null;
                if (fieldsTerm.length == 1) {
                    tuple = splitRow(lineBytes, fieldsTerm[0]);
                } else {
                    throw new RuntimeException("Fields terminated does not support multiple bytes");
                }
                insertTuples(tuple);
                lineBreakPos = i + 1;
            }
        }
        if (lineBreakPos <= len - 1) {
            if (preBytes != null) {
                int tmpLen = len - lineBreakPos;
                byte[] bytes1 = new byte[tmpLen + preBytes.length];
                System.arraycopy(preBytes, 0, bytes1, 0, preBytes.length);
                System.arraycopy(bytes, 0, bytes1, preBytes.length, tmpLen);
                preBytes = bytes1;
            } else {
                preBytes = new byte[len - lineBreakPos];
                System.arraycopy(bytes, lineBreakPos, preBytes, 0, preBytes.length);
            }
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
            log.error(e.getMessage(), e);
            if (e.getMessage().contains("epoch is not match, region_epoch")
                || e.getMessage().contains("Key out of range")) {
                if (!continueRetry()) {
                    throw e;
                }
                insertWithoutTxn(tuples, true);
            } else {
                throw e;
            }
        }
    }

    public void insertWithTxn(Object[] tuples) {
        List<KeyValue> caches = ExecutionEnvironment.memoryCache.computeIfAbsent(statementId, e -> new ArrayList<>());
        KeyValue keyValue = codec.encode(tuples);
        if (!caches.contains(keyValue)) {
            caches.add(keyValue);
        }
        if (caches.size() > 50000) {
            try {
                long startTs = TransactionManager.getStartTs();
                CommonId txnId = new CommonId(CommonId.CommonType.TRANSACTION,
                    TransactionManager.getServerId().seq, startTs);
                List<Object[]> tupleList = getCacheTupleList(caches, txnId);
                TxnImportDataOperation txnImportDataOperation = new TxnImportDataOperation(
                    startTs, txnId, txnRetry, txnRetryCnt, timeOut
                );
                int result = txnImportDataOperation.insertByTxn(tupleList);
                count.addAndGet(result);
            } finally {
                ExecutionEnvironment.memoryCache.remove(statementId);
            }
        }
    }

    public void endWriteWithTxn() {
        try {
            long startTs = TransactionManager.getStartTs();
            CommonId txnId = new CommonId(
                CommonId.CommonType.TRANSACTION,
                TransactionManager.getServerId().seq,
                startTs);
            TxnImportDataOperation txnImportDataOperation = new TxnImportDataOperation(
                startTs, txnId, txnRetry, txnRetryCnt, timeOut
            );
            List<KeyValue> caches = ExecutionEnvironment.memoryCache
                .computeIfAbsent(statementId, e -> new ArrayList<>());
            List<Object[]> tupleList = getCacheTupleList(caches, txnId);
            if (tupleList.size() == 0) {
                return;
            }
            int result = txnImportDataOperation.insertByTxn(tupleList);
            count.addAndGet(result);
        } finally {
            ExecutionEnvironment.memoryCache.remove(statementId);
        }
    }

    public List<Object[]> getCacheTupleList(List<KeyValue> keyValueList, CommonId txnId) {
        List<Object[]> tupleCacheList = new ArrayList<>();
        for (KeyValue keyValue : keyValueList) {
            tupleCacheList.add(getCacheTuples(keyValue, txnId));
        }
        return tupleCacheList;
    }

    public Object[] getCacheTuples(KeyValue keyValue, CommonId txnId) {
        CommonId partId = PartitionService.getService(
                Optional.ofNullable(table.getPartitionStrategy())
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
            .calcPartId(keyValue.getKey(), distributions);
        byte[] txnIdByte = txnId.encode();
        byte[] tableIdByte = table.getTableId().encode();
        byte[] partIdByte = partId.encode();

        keyValue.setKey(ByteUtils.encode(
            CommonId.CommonType.TXN_CACHE_DATA,
            keyValue.getKey(),
            Op.PUTIFABSENT.getCode(),
            (txnIdByte.length + tableIdByte.length + partIdByte.length),
            txnIdByte, tableIdByte, partIdByte));
        return io.dingodb.exec.utils.ByteUtils.decode(keyValue);
    }

    private static boolean continueRetry() {
        if (exceptionRetries > maxRetries) {
            return false;
        }
        try {
            Thread.sleep(retryInterval);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        exceptionRetries ++;
        return true;
    }

    public Object[] splitRow(byte[] bytes, byte terminated) throws UnsupportedEncodingException {
        if (lineStarting != null) {
            byte[] bytesTmp = new byte[bytes.length - lineStarting.length];
            System.arraycopy(bytes, lineStarting.length, bytesTmp, 0, bytesTmp.length);
            bytes = bytesTmp;
        }
        int len = bytes.length;
        int fieldBreakPos = 0;
        List<String> tupleList = new ArrayList<>();
        for (int i = 0; i < len; i ++) {
            byte b = bytes[i];
            if (b == terminated && i >= 1 && bytes[i - 1] != escaped[0]) {
                byte[] fieldBytes = new byte[i - fieldBreakPos];
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
        if (fieldBreakPos <= len - 1) {
            byte[] fieldBytes = new byte[len - fieldBreakPos];
            System.arraycopy(bytes, fieldBreakPos, fieldBytes, 0, fieldBytes.length);
            String valTmp = new String(fieldBytes, charset);
            if ("\\N".equalsIgnoreCase(valTmp)) {
                tupleList.add(valTmp);
            } else {
                tupleList.add(StringEscapeUtils.unescapeJson(valTmp));
            }
        } else if (bytes[len - 1] == terminated) {
            tupleList.add("");
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
        for (Column colDef : table.getColumns()) {
            if (colDef.getState() == 2 && colDef.isAutoIncrement()) {
                hasHide = true;
                break;
            }
        }
        if (hasHide && schema.fieldCount() - tuples.length == 1) {
            Object[] tupleTmp = new Object[schema.fieldCount()];
            System.arraycopy(tuples, 0, tupleTmp, 0, tuples.length);
            Long id = metaService.getAutoIncrement(table.getTableId());
            tupleTmp[tuples.length] = id.toString();
            return tupleTmp;
        } else {
            return tuples;
        }
    }

    private boolean checkEngine() {
        String engine = table.getEngine().toUpperCase();
        if (StringUtils.isNotBlank(engine) && engine.contains("TXN")) {
            return true;
        }
        return false;
    }

}
