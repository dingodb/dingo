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

import com.google.common.collect.Iterators;
import io.dingodb.codec.CodecService;
import io.dingodb.common.CommonId;
import io.dingodb.common.CoprocessorV2;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.Services;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.TxnIndexRangeScanParam;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.utils.RelOpUtils;
import io.dingodb.expr.rel.PipeOp;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Table;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

import static io.dingodb.common.util.NoBreakFunctions.wrap;
import static io.dingodb.exec.operator.TxnGetByIndexOperator.createGetLocal;
import static io.dingodb.exec.operator.TxnScanWithRelOpOperatorBase.createStoreIteratorCp;

@Slf4j
public class TxnIndexRangeScanOperator extends TxnScanOperatorBase {
    public static final TxnIndexRangeScanOperator INSTANCE = new TxnIndexRangeScanOperator();

    private static Object[] revMap(Object[] tuple, Vertex vertex) {
        TxnIndexRangeScanParam param = vertex.getParam();
        if (param.isLookup()) {
            return lookUp(tuple, param, vertex.getTask());
        } else {
            return transformTuple(tuple, param);
        }
    }

    public static Object[] lookUp(Object[] tuples, TxnIndexRangeScanParam param, Task task) {
        CommonId txnId = task.getTxnId();
        TransactionType transactionType = task.getTransactionType();
        TupleMapping indices = param.getKeyMapping();
        Table tableDefinition = param.getTable();
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> ranges =
            MetaService.root().getRangeDistribution(tableDefinition.tableId);
        Object[] keyTuples = new Object[tableDefinition.getColumns().size()];
        for (int i = 0; i < indices.getMappings().length; i ++) {
            keyTuples[indices.get(i)] = tuples[i];
        }
        byte[] keys = param.getLookupCodec().encodeKey(keyTuples);
        CommonId regionId = PartitionService.getService(
                Optional.ofNullable(tableDefinition.getPartitionStrategy())
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
            .calcPartId(keys, ranges);

        keys = CodecService.getDefault().setId(keys, regionId.domain);
        Object[] local = createGetLocal(
            keys,
            txnId,
            regionId,
            param.getTableId(),
            param.getLookupCodec(),
            transactionType);
        if (local != null) {
            return local;
        }

        StoreInstance store = Services.KV_STORE.getInstance(param.getTableId(), regionId);
        return param.getLookupCodec().decode(store.txnGet(param.getScanTs(), keys, param.getTimeout()));
    }

    private static Object[] transformTuple(Object[] tuple, TxnIndexRangeScanParam param) {
        Table table = param.getTable();
        List<Integer> mapList = param.getMapList();
        Object[] response = new Object[table.getColumns().size()];
        for (int i = 0; i < mapList.size(); i ++) {
            response[mapList.get(i)] = tuple[i];
        }
        return response;
    }

    @Override
    protected @NonNull Iterator<Object[]> createIterator(@NonNull Context context, @NonNull Vertex vertex) {
        TxnIndexRangeScanParam param = vertex.getParam();
        OperatorProfile profile = param.getProfile("indexFullScan");
        long start = System.currentTimeMillis();
        RangeDistribution distribution = context.getDistribution();

        Iterator<KeyValue> localIterator = createLocalIterator(
            vertex.getTask().getTxnId(),
            param.getIndexTableId(),
            distribution);
        if (localIterator.hasNext()) {
            Iterator<KeyValue> storeIterator = createStoreIterator(
                param.getIndexTableId(),
                distribution,
                param.getScanTs(),
                param.getTimeout()
            );
            param.setNullCoprocessor(distribution.getId());
            Iterator<Object[]> iterator = createMergedIterator(localIterator, storeIterator, param.getCodec());
            if (param.getRelOp() != null) {
                if (param.getRelOp() instanceof PipeOp) {
                    PipeOp op = (PipeOp) param.getRelOp();
                    iterator = Iterators.filter(iterator, tuple -> {
                        Object[] res = op.put(tuple);
                        return res != null;
                    });
                } else {
                    LogUtils.error(log, "index range scan cop is null,local is not empty, but rel op :{}", param.getRelOp());
                }
            }
            iterator = Iterators.transform(iterator, tuples -> revMap(tuples, vertex));
            if (param.getSelection() != null) {
                iterator = Iterators.transform(iterator, param.getSelection()::revMap);
            }
            return iterator;
        }

        CoprocessorV2 coprocessor = param.getCoprocessor();
        if (coprocessor == null) {
            Iterator<KeyValue> storeIterator = createStoreIterator(
                param.getIndexTableId(),
                distribution,
                param.getScanTs(),
                param.getTimeout()
            );
            Iterator<Object[]> iterator = Iterators.transform(storeIterator, wrap(param.getCodec()::decode)::apply);
            if (param.getRelOp() != null) {
                if (param.getRelOp() instanceof PipeOp) {
                    PipeOp op = (PipeOp) param.getRelOp();
                    iterator = Iterators.filter(iterator, tuple -> {
                        Object[] res = op.put(tuple);
                        return res != null;
                    });
                } else {
                    LogUtils.error(log, "index range scan cop is null, but rel op :{}", param.getRelOp());
                }
            }
            iterator = Iterators.transform(iterator, tuples -> revMap(tuples, vertex));
            if (param.getSelection() != null) {
                iterator = Iterators.transform(iterator, param.getSelection()::revMap);
            }
            return iterator;
        }

        Iterator<KeyValue> storeIterator = createStoreIteratorCp(
            param.getIndexTableId(),
            distribution,
            param.getScanTs(),
            param.getTimeout(),
            coprocessor
        );

        profile.time(start);
        Iterator<Object[]> iterator = Iterators.transform(storeIterator, wrap(param.getPushDownCodec()::decode)::apply);
        iterator = Iterators.transform(iterator, tuples -> revMap(tuples, vertex));
        if (param.getSelection() != null) {
            iterator = Iterators.transform(iterator, param.getSelection()::revMap);
        }
        return iterator;
    }

    @Override
    protected @NonNull Scanner getScanner(@NonNull Context context, @NonNull Vertex vertex) {
        //TxnIndexRangeScanParam param = vertex.getParam();
        return RelOpUtils::doScan;
        //if (param.getCoprocessor() != null || param.getRelOp() == null) {
        //    return RelOpUtils::doScan;
        //}
        //return RelOpUtils::doScanWithPipeOp;
    }

}
