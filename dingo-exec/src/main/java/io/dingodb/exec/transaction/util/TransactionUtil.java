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

package io.dingodb.exec.transaction.util;

import io.dingodb.common.CommonId;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.meta.MetaService;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.transaction.data.Mutation;
import io.dingodb.store.api.transaction.data.prewrite.LockExtraData;
import io.dingodb.store.api.transaction.data.prewrite.LockExtraDataList;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


@Slf4j
public class TransactionUtil {

    public static <T> List<Set<T>> splitSetIntoSubsets(Set<T> set, int batchSize) {
        List<T> tempList = new ArrayList<>(set);
        List<Set<T>> subsets = new ArrayList<>();
        for (int i = 0; i < tempList.size(); i += batchSize) {
            subsets.add(new HashSet<>(tempList.subList(i, Math.min(i + batchSize, tempList.size()))));
        }
        return subsets;
    }

    public static CommonId singleKeySplitRegionId(CommonId tableId, CommonId txnId, byte[] key) {
        // 2、regin split
        MetaService root = MetaService.root();
        TableDefinition tableDefinition = root.getTableDefinition(tableId);
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution = root.getRangeDistribution(tableId);
        CommonId regionId = PartitionService.getService(
                Optional.ofNullable(tableDefinition.getPartDefinition())
                    .map(PartitionDefinition::getFuncName)
                    .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
            .calcPartId(key, rangeDistribution);
        log.info("{} regin split retry tableId:{} regionId:{}", txnId, tableId, regionId);
        return regionId;
    }

    public static Map<CommonId, List<byte[]>> multiKeySplitRegionId(CommonId tableId, CommonId txnId, List<byte[]> keys) {
        // 2、regin split
        MetaService root = MetaService.root();
        TableDefinition tableDefinition = root.getTableDefinition(tableId);
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> rangeDistribution = root.getRangeDistribution(tableId);
        final PartitionService ps = PartitionService.getService(
            Optional.ofNullable(tableDefinition.getPartDefinition())
                .map(PartitionDefinition::getFuncName)
                .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME));
        Map<CommonId, List<byte[]>> partMap = ps.partKeys(keys, rangeDistribution);
        log.info("{} regin split retry tableId:{}", txnId, tableId);
        return partMap;
    }

    public static Map<CommonId, TableDefinition> getIndexDefinitions(CommonId tableId) {
        MetaService root = MetaService.root();
        return root.getTableIndexDefinitions(tableId);
    }
    public static List<byte[]> mutationToKey(List<Mutation> mutations) {
        List<byte[]> keys = new ArrayList<>(mutations.size());
        for (Mutation mutation:mutations) {
            keys.add(mutation.getKey());
        }
        return keys;
    }

    public static List<Mutation> keyToMutation(List<byte[]> keys, List<Mutation> srcMutations) {
        List<Mutation> mutations = new ArrayList<>(keys.size());
        for (Mutation mutation: srcMutations) {
            if (keys.contains(mutation.getKey())) {
                mutations.add(mutation);
            }
        }
        return mutations;
    }

    public static List<LockExtraData> toLockExtraDataList(CommonId tableId, CommonId partId, CommonId txnId, int transactionType, int size) {
        LockExtraDataList lockExtraData = LockExtraDataList.builder().
            tableId(tableId)
            .partId(partId)
            .serverId(TransactionManager.getServerId())
            .txnId(txnId)
            .transactionType(transactionType).build();
        byte[] encode = lockExtraData.encode();
        List<LockExtraData> lockExtraDataList = IntStream.range(0, size)
            .mapToObj(i -> new LockExtraData(i, encode))
            .collect(Collectors.toList());
        return lockExtraDataList;
    }
}
