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

package io.dingodb.server.executor.common;

import io.dingodb.common.util.Optional;
import io.dingodb.sdk.common.partition.Partition;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.Table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableDefinition implements Table {

    private final io.dingodb.common.table.TableDefinition tableDefinition;

    public TableDefinition(io.dingodb.common.table.TableDefinition tableDefinition) {
        this.tableDefinition = tableDefinition;
    }

    @Override
    public String getName() {
        return tableDefinition.getName();
    }

    @Override
    public List<Column> getColumns() {
        return tableDefinition.getColumns().stream().map(ColumnDefinition::new).collect(Collectors.toList());
    }

    @Override
    public int getVersion() {
        return tableDefinition.getVersion();
    }

    @Override
    public int getTtl() {
        return tableDefinition.getTtl();
    }

    @Override
    public Partition getPartDefinition() {
        if (tableDefinition.getPartDefinition() == null) {
            return null;
        }
        return new PartitionRule(tableDefinition.getPartDefinition());
    }

    @Override
    public String getEngine() {
        return Optional.ofNullable(tableDefinition.getEngine()).orElse("ENG_ROCKSDB");
    }

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> map = new HashMap<>();
        tableDefinition.getProperties().forEach((key, value) -> map.put((String) key, (String) value));
        return map;
    }

    @Override
    public long autoIncrement() {
        return tableDefinition.getAutoIncrement();
    }

    @Override
    public int getReplica() {
        return 1;
    }
}
