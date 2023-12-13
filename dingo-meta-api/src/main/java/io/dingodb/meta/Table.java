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

package io.dingodb.meta;

import io.dingodb.common.CommonId;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.ToString;

import java.util.List;
import java.util.Properties;

@Builder
@ToString
@AllArgsConstructor
public class Table {

    public final CommonId tableId;
    public final List<Column> columns;

    public final int replica;
    public final String partitionStrategy;
    public final List<Partition> partitions;

    public final String engine;
    public final int version;

    public final Properties properties;

    public final List<Table> indexes;

}
