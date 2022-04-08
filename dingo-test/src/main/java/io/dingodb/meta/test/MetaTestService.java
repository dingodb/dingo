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

package io.dingodb.meta.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.table.TableId;
import io.dingodb.exec.Services;
import io.dingodb.exec.table.PartInKvStore;
import io.dingodb.meta.Location;
import io.dingodb.meta.LocationGroup;
import io.dingodb.meta.MetaService;
import io.dingodb.store.api.StoreInstance;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Slf4j
public class MetaTestService implements MetaService {
    public static final String SCHEMA_NAME = "TEST";

    public static final MetaTestService INSTANCE = new MetaTestService();
    private final Map<String, TableDefinition> tableDefinitionMap = new LinkedHashMap<>();
    private File dataPath0;
    private File dataPath1;
    private File dataPath2;

    @Nonnull
    private static File tempPath() {
        try {
            return Files.createTempDirectory("dingo_file_").toFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getName() {
        return SCHEMA_NAME;
    }

    @Override
    public void init(@Nullable Map<String, Object> props) {
        dataPath0 = tempPath();
        dataPath1 = tempPath();
        dataPath2 = tempPath();
    }

    @Override
    public void clear() {
        try {
            FileUtils.deleteDirectory(dataPath0);
            FileUtils.deleteDirectory(dataPath1);
            FileUtils.deleteDirectory(dataPath2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        tableDefinitionMap.clear();
    }

    @Override
    public byte[] getTableKey(@Nonnull String tableName) {
        return tableName.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getIndexId(@Nonnull String tableName) {
        return new byte[0];
    }

    @Override
    public void createTable(@Nonnull String tableName, @Nonnull TableDefinition tableDefinition) {
        tableDefinitionMap.put(tableName, tableDefinition);
        Map<String, Location> partLocations = getPartLocations(tableName);
        for (Map.Entry<String, Location> entry : partLocations.entrySet()) {
            StoreInstance store = Services.KV_STORE.getInstance(entry.getValue().getPath());
            new PartInKvStore(
                store.getKvBlock(new TableId(getTableKey(tableName)), entry.getKey()),
                tableDefinition.getTupleSchema(),
                tableDefinition.getKeyMapping()
            );
        }
    }

    @Override
    public boolean dropTable(@Nonnull String tableName) {
        if (tableDefinitionMap.containsKey(tableName)) {
            tableDefinitionMap.remove(tableName);
            return true;
        }
        return false;
    }

    @Override
    public Map<String, TableDefinition> getTableDefinitions() {
        return tableDefinitionMap;
    }

    @Override
    public Location currentLocation() {
        return new Location("localhost", 0, dataPath0.getAbsolutePath());
    }

    @Override
    public Map<String, Location> getPartLocations(String name) {
        return ImmutableMap.of(
            "0", new Location("localhost", 0, dataPath0.getAbsolutePath()),
            "1", new Location("localhost", 0, dataPath1.getAbsolutePath()),
            "2", new Location("localhost", 0, dataPath2.getAbsolutePath())
        );
    }

    @Override
    public LocationGroup getLocationGroup(String name) {
        return new LocationGroup(
            new Location("localhost", 0, dataPath0.getAbsolutePath()),
            Lists.newArrayList(
                new Location("localhost", 0, dataPath0.getAbsolutePath()),
                new Location("localhost", 0, dataPath0.getAbsolutePath()),
                new Location("localhost", 0, dataPath0.getAbsolutePath())
            ));
    }
}
