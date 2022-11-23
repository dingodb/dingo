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

package io.dingodb.test;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.Part;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import javax.annotation.Nonnull;

@Slf4j
public class MetaTestService implements MetaService {
    public static final String SCHEMA_NAME = "TEST";

    public static final MetaTestService INSTANCE = new MetaTestService();
    private final Map<String, TableDefinition> tableDefinitionMap = new LinkedHashMap<>();

    @Override
    public CommonId id() {
        return null;
    }

    @Override
    public String name() {
        return SCHEMA_NAME;
    }

    public void clear() {
        tableDefinitionMap.keySet().stream()
            .map(name -> new CommonId(
                (byte) 'T',
                new byte[]{'D', 'T'},
                0,
                name.hashCode()
            )).forEach(StoreTestServiceProvider.STORE_SERVICE::deleteInstance);
        tableDefinitionMap.clear();
    }

    @Override
    public void createTable(@Nonnull @NonNull String tableName, @Nonnull @NonNull TableDefinition tableDefinition) {
        tableDefinitionMap.put(tableName, tableDefinition);
    }

    @Override
    public void createTable(CommonId id, @NonNull String tableName, @NonNull TableDefinition tableDefinition) {

    }

    @Override
    public boolean dropTable(@Nonnull @NonNull String tableName) {
        if (tableDefinitionMap.containsKey(tableName)) {
            tableDefinitionMap.remove(tableName);
            return true;
        }
        return false;
    }

    @Override
    public boolean dropTable(CommonId id, @NonNull String tableName) {
        return false;
    }

    @Override
    public CommonId getTableId(@Nonnull @NonNull String tableName) {
        return new CommonId(
            (byte) 'T',
            new byte[]{'D', 'T'},
            0,
            tableName.hashCode()
        );
    }

    @Override
    public CommonId getTableId(CommonId id, @NonNull String tableName) {
        return null;
    }

    @Override
    public TableDefinition getTableDefinition(CommonId id, @NonNull String name) {
        return null;
    }

    @Override
    public Map<String, TableDefinition> getTableDefinitions() {
        return tableDefinitionMap;
    }

    @Override
    public Map<String, TableDefinition> getTableDefinitions(CommonId id) {
        return null;
    }

    @Override
    public NavigableMap<ComparableByteArray, Part> getParts(String name) {
        TreeMap<ComparableByteArray, Part> result = new TreeMap<>();
        byte[] startKey = ByteArrayUtils.EMPTY_BYTES;
        byte[] endKey = ByteArrayUtils.MAX_BYTES;
        result.put(new ComparableByteArray(startKey), new Part(
            startKey,
            new FakeLocation(1),
            Collections.singletonList(new FakeLocation(1)),
            startKey,
            endKey
        ));
        return result;
    }

    @Override
    public NavigableMap<ComparableByteArray, Part> getParts(CommonId id, String name) {
        return null;
    }

    @Override
    public List<Location> getDistributes(String name) {
        return Collections.singletonList(new FakeLocation(1));
    }

    @Override
    public Location currentLocation() {
        return new FakeLocation(0);
    }

    @Override
    public void createSubMetaService(CommonId id, String name) {

    }

    @Override
    public Map<String, MetaService> getSubMetaServices(CommonId id) {
        return Collections.singletonMap(SCHEMA_NAME, new MetaTestService());
    }

    @Override
    public MetaService getSubMetaService(CommonId id, String name) {
        return null;
    }

    @Override
    public boolean dropSubMetaService(CommonId id, String name) {
        return false;
    }
}
