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

package io.dingodb.partition.base;

import com.google.auto.service.AutoService;
import io.dingodb.partition.PartitionService;

@AutoService(io.dingodb.partition.DingoPartitionServiceProvider.class)
public class DingoPartitionServiceProvider implements io.dingodb.partition.DingoPartitionServiceProvider {
    RangePartitionService rangePartitionService = new RangePartitionService();
    HashRangePartitionService hashRangePartitionService= new HashRangePartitionService();

    @Override
    public PartitionService getService(String funcName) {
        funcName = funcName == null ? RANGE_FUNC_NAME : funcName;
        switch (funcName.toUpperCase()) {
            case RANGE_FUNC_NAME:
                return rangePartitionService;
            case HASH_FUNC_NAME:
                return  hashRangePartitionService;
            default:
                throw new IllegalStateException(UNSUPPORTED_PARTITION_FUNC_MSG + funcName);
        }
    }
}
