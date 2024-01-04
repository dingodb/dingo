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

package io.dingodb.client.common;

import io.dingodb.client.vector.Partitions;
import io.dingodb.sdk.service.AutoIncrementService;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.IndexDefinition;
import io.dingodb.sdk.service.entity.meta.RangeDistribution;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.NavigableMap;

import static io.dingodb.common.util.ByteArrayUtils.SKIP_LONG_POS;

@Slf4j
@AllArgsConstructor
public class IndexInfo {

    public final String schemaName;
    public final String indexName;
    public final DingoCommonId indexId;


    public final IndexDefinition index;
    public final KeyValueCodec codec;
    public final AutoIncrementService autoIncrementService;
    public final Partitions partitions;
    public final List<RangeDistribution> distributions;

}
