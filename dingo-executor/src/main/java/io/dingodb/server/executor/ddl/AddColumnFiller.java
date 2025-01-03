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
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.Services;
import io.dingodb.exec.transaction.base.CacheToObject;
import io.dingodb.exec.transaction.base.TxnLocalData;
import io.dingodb.exec.transaction.util.TransactionCacheToMutation;
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
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;
import java.util.NavigableMap;
import java.util.stream.Collectors;

import static io.dingodb.common.CommonId.CommonType.FILL_BACK;
import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public class AddColumnFiller extends IndexAddFiller {
    private Object defaultVal = null;

    boolean withoutPrimary;

    private CommonId replicaId;

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
        columnIndices = table.getColumnIndices(indexTable.columns.stream()
            .map(Column::getName)
            .collect(Collectors.toList()));
        colLen = columnIndices.size();
        if (columnIndices.contains(-1)) {
            defaultVal = addColumn.getDefaultVal();
            columnIndices.removeIf(index -> index == -1);
            colLen = columnIndices.size();
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
        Object[] tuplesTmp = new Object[colLen + 1];
        for (int i = 0; i < colLen; i++) {
            tuplesTmp[i] = tuples[columnIndices.get(i)];
        }
        if (withoutPrimary) {
            tuplesTmp[colLen] = tuplesTmp[colLen - 1];
            tuplesTmp[colLen - 1] = defaultVal;
        } else {
            tuplesTmp[colLen] = defaultVal;
        }
        return tuplesTmp;
    }

    @Override
    public TxnLocalData getTxnLocalData(Object[] tuples) {
        Object[] tuplesTmp = getNewTuples(colLen, tuples);
        KeyValue keyValue = wrap(indexCodec::encode).apply(tuplesTmp);
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
