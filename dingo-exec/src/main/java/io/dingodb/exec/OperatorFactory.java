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

package io.dingodb.exec;

import io.dingodb.common.CommonId;
import io.dingodb.exec.base.Operator;
import io.dingodb.exec.operator.AggregateOperator;
import io.dingodb.exec.operator.CacheOpOperator;
import io.dingodb.exec.operator.CalcDistributionOperator;
import io.dingodb.exec.operator.CoalesceOperator;
import io.dingodb.exec.operator.CompareAndSetOperator;
import io.dingodb.exec.operator.CopyOperator;
import io.dingodb.exec.operator.DistributeOperator;
import io.dingodb.exec.operator.EmptySourceOperator;
import io.dingodb.exec.operator.ExportDataOperator;
import io.dingodb.exec.operator.FilterOperator;
import io.dingodb.exec.operator.GetByIndexOperator;
import io.dingodb.exec.operator.GetByKeysOperator;
import io.dingodb.exec.operator.GetDistributionOperator;
import io.dingodb.exec.operator.HashJoinOperator;
import io.dingodb.exec.operator.HashOperator;
import io.dingodb.exec.operator.IndexMergeOperator;
import io.dingodb.exec.operator.InfoSchemaScanOperator;
import io.dingodb.exec.operator.LikeScanOperator;
import io.dingodb.exec.operator.NewCalcDistributionOperator;
import io.dingodb.exec.operator.PartCountOperator;
import io.dingodb.exec.operator.PartDeleteOperator;
import io.dingodb.exec.operator.PartInsertOperator;
import io.dingodb.exec.operator.PartRangeDeleteOperator;
import io.dingodb.exec.operator.PartRangeScanOperator;
import io.dingodb.exec.operator.PartUpdateOperator;
import io.dingodb.exec.operator.PartVectorOperator;
import io.dingodb.exec.operator.PartitionOperator;
import io.dingodb.exec.operator.PessimisticLockDeleteOperator;
import io.dingodb.exec.operator.PessimisticLockInsertOperator;
import io.dingodb.exec.operator.PessimisticLockOperator;
import io.dingodb.exec.operator.PessimisticLockUpdateOperator;
import io.dingodb.exec.operator.PipeOpOperator;
import io.dingodb.exec.operator.ProjectOperator;
import io.dingodb.exec.operator.ReceiveOperator;
import io.dingodb.exec.operator.ReduceOperator;
import io.dingodb.exec.operator.ReduceRelOpOperator;
import io.dingodb.exec.operator.RemovePartOperator;
import io.dingodb.exec.operator.RootOperator;
import io.dingodb.exec.operator.ScanOperator;
import io.dingodb.exec.operator.ScanWithCacheOpOperator;
import io.dingodb.exec.operator.ScanWithPipeOpOperator;
import io.dingodb.exec.operator.SendOperator;
import io.dingodb.exec.operator.SortOperator;
import io.dingodb.exec.operator.SumUpOperator;
import io.dingodb.exec.operator.TxnGetByIndexOperator;
import io.dingodb.exec.operator.TxnGetByKeysOperator;
import io.dingodb.exec.operator.TxnLikeScanOperator;
import io.dingodb.exec.operator.TxnPartDeleteOperator;
import io.dingodb.exec.operator.TxnPartInsertOperator;
import io.dingodb.exec.operator.TxnPartRangeDeleteOperator;
import io.dingodb.exec.operator.TxnPartRangeScanOperator;
import io.dingodb.exec.operator.TxnPartUpdateOperator;
import io.dingodb.exec.operator.TxnPartVectorOperator;
import io.dingodb.exec.operator.TxnScanOperator;
import io.dingodb.exec.operator.TxnScanWithCacheOpOperator;
import io.dingodb.exec.operator.TxnScanWithPipeOpOperator;
import io.dingodb.exec.operator.ValuesOperator;
import io.dingodb.exec.operator.VectorPartitionOperator;
import io.dingodb.exec.operator.VectorPointDistanceOperator;
import io.dingodb.exec.transaction.operator.CleanCacheOperator;
import io.dingodb.exec.transaction.operator.CommitOperator;
import io.dingodb.exec.transaction.operator.PessimisticResidualLockOperator;
import io.dingodb.exec.transaction.operator.PessimisticRollBackOperator;
import io.dingodb.exec.transaction.operator.PreWriteOperator;
import io.dingodb.exec.transaction.operator.RollBackOperator;
import io.dingodb.exec.transaction.operator.ScanCacheOperator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.dingodb.exec.utils.OperatorCodeUtils.AGGREGATE;
import static io.dingodb.exec.utils.OperatorCodeUtils.CACHE_OP;
import static io.dingodb.exec.utils.OperatorCodeUtils.CALC_DISTRIBUTION;
import static io.dingodb.exec.utils.OperatorCodeUtils.CALC_DISTRIBUTION_1;
import static io.dingodb.exec.utils.OperatorCodeUtils.COALESCE;
import static io.dingodb.exec.utils.OperatorCodeUtils.COMMIT;
import static io.dingodb.exec.utils.OperatorCodeUtils.COMPARE_AND_SET;
import static io.dingodb.exec.utils.OperatorCodeUtils.COPY;
import static io.dingodb.exec.utils.OperatorCodeUtils.DISTRIBUTE;
import static io.dingodb.exec.utils.OperatorCodeUtils.EMPTY_SOURCE;
import static io.dingodb.exec.utils.OperatorCodeUtils.EXPORT_DATA;
import static io.dingodb.exec.utils.OperatorCodeUtils.FILTER;
import static io.dingodb.exec.utils.OperatorCodeUtils.GET_BY_INDEX;
import static io.dingodb.exec.utils.OperatorCodeUtils.GET_BY_KEYS;
import static io.dingodb.exec.utils.OperatorCodeUtils.GET_DISTRIBUTION;
import static io.dingodb.exec.utils.OperatorCodeUtils.HASH;
import static io.dingodb.exec.utils.OperatorCodeUtils.HASH_JOIN;
import static io.dingodb.exec.utils.OperatorCodeUtils.INDEX_MERGE;
import static io.dingodb.exec.utils.OperatorCodeUtils.INFO_SCHEMA_SCAN;
import static io.dingodb.exec.utils.OperatorCodeUtils.LIKE_SCAN;
import static io.dingodb.exec.utils.OperatorCodeUtils.PARTITION;
import static io.dingodb.exec.utils.OperatorCodeUtils.PART_COUNT;
import static io.dingodb.exec.utils.OperatorCodeUtils.PART_DELETE;
import static io.dingodb.exec.utils.OperatorCodeUtils.PART_INSERT;
import static io.dingodb.exec.utils.OperatorCodeUtils.PART_RANGE_DELETE;
import static io.dingodb.exec.utils.OperatorCodeUtils.PART_RANGE_SCAN;
import static io.dingodb.exec.utils.OperatorCodeUtils.PART_UPDATE;
import static io.dingodb.exec.utils.OperatorCodeUtils.PART_VECTOR;
import static io.dingodb.exec.utils.OperatorCodeUtils.PESSIMISTIC_LOCK;
import static io.dingodb.exec.utils.OperatorCodeUtils.PESSIMISTIC_LOCK_DELETE;
import static io.dingodb.exec.utils.OperatorCodeUtils.PESSIMISTIC_LOCK_INSERT;
import static io.dingodb.exec.utils.OperatorCodeUtils.PESSIMISTIC_LOCK_UPDATE;
import static io.dingodb.exec.utils.OperatorCodeUtils.PESSIMISTIC_RESIDUAL_LOCK;
import static io.dingodb.exec.utils.OperatorCodeUtils.PESSIMISTIC_ROLL_BACK;
import static io.dingodb.exec.utils.OperatorCodeUtils.PIPE_OP;
import static io.dingodb.exec.utils.OperatorCodeUtils.PRE_WRITE;
import static io.dingodb.exec.utils.OperatorCodeUtils.PROJECT;
import static io.dingodb.exec.utils.OperatorCodeUtils.RECEIVE;
import static io.dingodb.exec.utils.OperatorCodeUtils.REDUCE;
import static io.dingodb.exec.utils.OperatorCodeUtils.REDUCE_REL_OP;
import static io.dingodb.exec.utils.OperatorCodeUtils.REMOVE_PART;
import static io.dingodb.exec.utils.OperatorCodeUtils.ROLL_BACK;
import static io.dingodb.exec.utils.OperatorCodeUtils.ROOT;
import static io.dingodb.exec.utils.OperatorCodeUtils.SCAN_CACHE;
import static io.dingodb.exec.utils.OperatorCodeUtils.SCAN_WITH_CACHE_OP;
import static io.dingodb.exec.utils.OperatorCodeUtils.SCAN_WITH_NO_OP;
import static io.dingodb.exec.utils.OperatorCodeUtils.SCAN_WITH_PIPE_OP;
import static io.dingodb.exec.utils.OperatorCodeUtils.SEND;
import static io.dingodb.exec.utils.OperatorCodeUtils.SORT;
import static io.dingodb.exec.utils.OperatorCodeUtils.SUM_UP;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_CLEAN_CACHE;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_GET_BY_INDEX;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_GET_BY_KEYS;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_LIKE_SCAN;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_PART_DELETE;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_PART_INSERT;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_PART_RANGE_DELETE;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_PART_RANGE_SCAN;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_PART_UPDATE;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_PART_VECTOR;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_SCAN_WITH_CACHE_OP;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_SCAN_WITH_NO_OP;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_SCAN_WITH_PIPE_OP;
import static io.dingodb.exec.utils.OperatorCodeUtils.VALUES;
import static io.dingodb.exec.utils.OperatorCodeUtils.VECTOR_PARTITION;
import static io.dingodb.exec.utils.OperatorCodeUtils.VECTOR_POINT_DISTANCE;

public final class OperatorFactory {

    private static final Map<CommonId, Operator> OPERATORS = new ConcurrentHashMap<>();

    static {
        OPERATORS.put(AGGREGATE, AggregateOperator.INSTANCE);
        OPERATORS.put(COALESCE, CoalesceOperator.INSTANCE);
        OPERATORS.put(CALC_DISTRIBUTION, CalcDistributionOperator.INSTANCE);
        OPERATORS.put(CALC_DISTRIBUTION_1, NewCalcDistributionOperator.INSTANCE);
        OPERATORS.put(EMPTY_SOURCE, EmptySourceOperator.INSTANCE);
        OPERATORS.put(FILTER, FilterOperator.INSTANCE);
        OPERATORS.put(GET_BY_INDEX, GetByIndexOperator.INSTANCE);
        OPERATORS.put(GET_BY_KEYS, GetByKeysOperator.INSTANCE);
        OPERATORS.put(HASH_JOIN, HashJoinOperator.INSTANCE);
        OPERATORS.put(HASH, HashOperator.INSTANCE);
        OPERATORS.put(INDEX_MERGE, IndexMergeOperator.INSTANCE);
        OPERATORS.put(LIKE_SCAN, LikeScanOperator.INSTANCE);
        OPERATORS.put(PART_COUNT, PartCountOperator.INSTANCE);
        OPERATORS.put(PART_DELETE, PartDeleteOperator.INSTANCE);
        OPERATORS.put(PART_INSERT, PartInsertOperator.INSTANCE);
        OPERATORS.put(PARTITION, PartitionOperator.INSTANCE);
        OPERATORS.put(PART_RANGE_DELETE, PartRangeDeleteOperator.INSTANCE);
        OPERATORS.put(PART_RANGE_SCAN, PartRangeScanOperator.INSTANCE);
        OPERATORS.put(PART_UPDATE, PartUpdateOperator.INSTANCE);
        OPERATORS.put(PART_VECTOR, PartVectorOperator.INSTANCE);
        OPERATORS.put(PROJECT, ProjectOperator.INSTANCE);
        OPERATORS.put(RECEIVE, ReceiveOperator.INSTANCE);
        OPERATORS.put(REDUCE, ReduceOperator.INSTANCE);
        OPERATORS.put(REDUCE_REL_OP, ReduceRelOpOperator.INSTANCE);
        OPERATORS.put(REMOVE_PART, RemovePartOperator.INSTANCE);
        OPERATORS.put(ROOT, RootOperator.INSTANCE);
        OPERATORS.put(PIPE_OP, PipeOpOperator.INSTANCE);
        OPERATORS.put(CACHE_OP, CacheOpOperator.INSTANCE);
        OPERATORS.put(SCAN_WITH_NO_OP, ScanOperator.INSTANCE);
        OPERATORS.put(SCAN_WITH_PIPE_OP, ScanWithPipeOpOperator.INSTANCE);
        OPERATORS.put(SCAN_WITH_CACHE_OP, ScanWithCacheOpOperator.INSTANCE);
        OPERATORS.put(SEND, SendOperator.INSTANCE);
        OPERATORS.put(SORT, SortOperator.INSTANCE);
        OPERATORS.put(SUM_UP, SumUpOperator.INSTANCE);
        OPERATORS.put(VALUES, ValuesOperator.INSTANCE);
        OPERATORS.put(VECTOR_PARTITION, VectorPartitionOperator.INSTANCE);
        OPERATORS.put(VECTOR_POINT_DISTANCE, VectorPointDistanceOperator.INSTANCE);
        OPERATORS.put(TXN_LIKE_SCAN, TxnLikeScanOperator.INSTANCE);
        OPERATORS.put(TXN_PART_RANGE_SCAN, TxnPartRangeScanOperator.INSTANCE);
        OPERATORS.put(TXN_PART_RANGE_DELETE, TxnPartRangeDeleteOperator.INSTANCE);
        OPERATORS.put(TXN_PART_UPDATE, TxnPartUpdateOperator.INSTANCE);
        OPERATORS.put(TXN_PART_INSERT, TxnPartInsertOperator.INSTANCE);
        OPERATORS.put(TXN_PART_DELETE, TxnPartDeleteOperator.INSTANCE);
        OPERATORS.put(TXN_SCAN_WITH_NO_OP, TxnScanOperator.INSTANCE);
        OPERATORS.put(TXN_SCAN_WITH_PIPE_OP, TxnScanWithPipeOpOperator.INSTANCE);
        OPERATORS.put(TXN_SCAN_WITH_CACHE_OP, TxnScanWithCacheOpOperator.INSTANCE);
        OPERATORS.put(COMMIT, CommitOperator.INSTANCE);
        OPERATORS.put(PRE_WRITE, PreWriteOperator.INSTANCE);
        OPERATORS.put(ROLL_BACK, RollBackOperator.INSTANCE);
        OPERATORS.put(SCAN_CACHE, ScanCacheOperator.INSTANCE);
        OPERATORS.put(INFO_SCHEMA_SCAN, InfoSchemaScanOperator.INSTANCE);
        OPERATORS.put(COMPARE_AND_SET, CompareAndSetOperator.INSTANCE);
        OPERATORS.put(EXPORT_DATA, ExportDataOperator.INSTANCE);
        OPERATORS.put(PESSIMISTIC_LOCK_DELETE, PessimisticLockDeleteOperator.INSTANCE);
        OPERATORS.put(PESSIMISTIC_LOCK_INSERT, PessimisticLockInsertOperator.INSTANCE);
        OPERATORS.put(PESSIMISTIC_LOCK_UPDATE, PessimisticLockUpdateOperator.INSTANCE);
        OPERATORS.put(PESSIMISTIC_ROLL_BACK, PessimisticRollBackOperator.INSTANCE);
        OPERATORS.put(DISTRIBUTE, DistributeOperator.INSTANCE);
        OPERATORS.put(GET_DISTRIBUTION, GetDistributionOperator.INSTANCE);
        OPERATORS.put(TXN_CLEAN_CACHE, CleanCacheOperator.INSTANCE);
        OPERATORS.put(COPY, CopyOperator.INSTANCE);
        OPERATORS.put(TXN_GET_BY_KEYS, TxnGetByKeysOperator.INSTANCE);
        OPERATORS.put(TXN_PART_VECTOR, TxnPartVectorOperator.INSTANCE);
        OPERATORS.put(TXN_GET_BY_INDEX, TxnGetByIndexOperator.INSTANCE);
        OPERATORS.put(PESSIMISTIC_LOCK, PessimisticLockOperator.INSTANCE);
        OPERATORS.put(PESSIMISTIC_RESIDUAL_LOCK, PessimisticResidualLockOperator.INSTANCE);
    }

    private OperatorFactory() {
    }

    public static Operator getInstance(CommonId id) {
        return OPERATORS.get(id);
    }
}
