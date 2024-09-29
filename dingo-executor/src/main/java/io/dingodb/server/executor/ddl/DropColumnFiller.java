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

import io.dingodb.common.CommonId;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.MetaService;
import lombok.extern.slf4j.Slf4j;

import java.util.NavigableMap;

import static io.dingodb.common.CommonId.CommonType.TABLE;

@Slf4j
public class DropColumnFiller extends IndexAddFiller {
    private CommonId replicaId;

    @Override
    public void initFiller() {
        super.initFiller();
        replicaId = indexTable.tableId;
        LogUtils.info(log, "replicaTableId:{}", replicaId);
    }

    @Override
    public NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> getRegionList() {
        return MetaService.root().getRangeDistribution(replicaId);
    }
}
