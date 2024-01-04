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

package io.dingodb.client.vector;

import io.dingodb.partition.base.ConsistentHashing;
import io.dingodb.sdk.common.codec.CodecUtils;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.PartitionStrategy;
import io.dingodb.sdk.service.entity.meta.RangeDistribution;

import java.util.List;
import java.util.TreeMap;

public class Partitions {

    private final ConsistentHashing<Long> hashing;
    private final TreeMap<Long, TreeMap<Long, RangeDistribution>> partitionRegions = new TreeMap<>();

    public Partitions(PartitionStrategy strategy, List<RangeDistribution> distributions) {
        if (strategy == PartitionStrategy.PT_STRATEGY_HASH) {
            hashing = new ConsistentHashing<>(3);
            for (RangeDistribution distribution : distributions) {
                hashing.addNode(distribution.getId().getParentEntityId());
                partitionRegions
                    .computeIfAbsent(distribution.getId().getParentEntityId(), k -> new TreeMap<>())
                    .put(VectorKeyCodec.decode(distribution.getRange().getStartKey()), distribution);
            }
        } else {
            hashing = null;
            for (RangeDistribution distribution : distributions) {
                partitionRegions
                    .computeIfAbsent(VectorKeyCodec.decode(distribution.getRange().getStartKey()), k -> new TreeMap<>())
                    .put(VectorKeyCodec.decode(distribution.getRange().getStartKey()), distribution);
            }
        }

    }

    public DingoCommonId lookup(byte[] key, long id) {
        long partition = hashing == null ? id : hashing.getNode(key);
        return partitionRegions.floorEntry(partition).getValue().floorEntry(id).getValue().getId();
    }

    public RangeDistribution lookupDistribution(byte[] key, long id) {
        long partition = hashing == null ? id : hashing.getNode(key);
        return partitionRegions.get(partition).floorEntry(id).getValue();
    }

    public static int search(long[] array, long target) {
        if (array.length == 1) {
            return 0;
        }
        if (array.length == 2) {
            return target == array[0] ? 0 : 1;
        }
        int left = 0;
        int right = array.length - 1;
        int closestIdx = -1;

        while (left <= right) {
            int mid = left + (right - left) / 2;
            if (array[mid] == target) {
                return mid;
            } else if (array[mid] < target) {
                closestIdx = mid;
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        return closestIdx;
    }

}
