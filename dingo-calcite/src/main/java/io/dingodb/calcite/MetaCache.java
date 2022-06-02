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

package io.dingodb.calcite;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.Services;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.Part;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptTable;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

@Slf4j
public class MetaCache {
    private static final ReadWriteLock lock = new ReentrantReadWriteLock();
    private static final Lock writeLock = lock.writeLock();
    private static final Lock readLock = lock.readLock();

    private static Map<String, TableDefinition> tableDefinitionsMap;

    private static MetaService metaService;

    private Map<String, CommonId> tableIdMap;

    private Map<String, NavigableMap<ByteArrayUtils.ComparableByteArray, Part>> tablePartMap;

    static {
        for (MetaService ms : Services.metaServices.values()) {
            log.info("MetaCache, static, metaservice name: {}.",
                ms.getName());
            metaService = ms;
        }
        final long startTime = System.currentTimeMillis();
        tableDefinitionsMap = metaService.getTableDefinitions();
    }

    public MetaCache() {
        tableIdMap = new HashMap<>();
        tablePartMap = new HashMap<>();
    }

    public TableDefinition getTableDefinition(final String tableName) {
        return getTableDefinitionsMap().get(tableName);
    }

    public NavigableMap<ByteArrayUtils.ComparableByteArray, Part> getParts(final String tableName) {
        if (this.tablePartMap.containsKey(tableName)) {
            return this.tablePartMap.get(tableName);
        }
        NavigableMap<ByteArrayUtils.ComparableByteArray, Part> mp = metaService.getParts(tableName);
        this.tablePartMap.put(tableName, mp);
        return mp;
    }

    public List<Location> getDistributes(final String tableName) {
        return this.getParts(tableName).values().stream()
            .map(Part::getLeader)
            .distinct()
            .collect(Collectors.toList());
    }

    public CommonId getTableId(final String tableName) {
        if (this.tableIdMap.containsKey(tableName)) {
            return this.tableIdMap.get(tableName);
        }
        CommonId cId = metaService.getTableId(tableName);
        this.tableIdMap.put(tableName, cId);
        return cId;
    }

    public static void initTableDefinitions() {
        Map<String, TableDefinition> tdMap = metaService.getTableDefinitions();
        writeLock.lock();
        try {
            tableDefinitionsMap = tdMap;
        } finally {
            writeLock.unlock();
        }
    }

    public static Map<String, TableDefinition> getTableDefinitionsMap() {
        readLock.lock();
        try {
            return tableDefinitionsMap;
        } finally {
            readLock.unlock();
        }
    }

    public static String getTableName(@Nonnull RelOptTable table) {
        List<String> fullName = table.getQualifiedName();
        return fullName.stream().skip(2).collect(Collectors.joining("."));
    }
}
