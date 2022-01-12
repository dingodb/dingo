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
import io.dingodb.exec.Services;
import io.dingodb.exec.table.PartInKvStore;
import io.dingodb.kvstore.KvStoreInstance;
import io.dingodb.meta.Location;
import io.dingodb.meta.LocationGroup;
import io.dingodb.meta.MetaService;
import io.dingodb.net.netty.NetServiceConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Slf4j
public class MetaTestService implements MetaService {
    public static final MetaTestService INSTANCE = new MetaTestService();
    public static final String PROP_PATH = "path";
    private File path;
    private File dataPath0;
    private File dataPath1;
    private File dataPath2;
    private boolean temporary = false;
    private Map<String, TableDefinition> tableDefinitionMap;

    @Nonnull
    private static File tempPath() {
        try {
            return Files.createTempDirectory("dingo_file_").toFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    private File metaFile(@Nonnull String tableName) {
        return new File(path, tableName.toLowerCase().replace(".", File.separator) + ".json");
    }

    @Override
    public void init(@Nullable Map<String, Object> props) {
        String pathName = props != null ? (String) props.get(PROP_PATH) : null;
        if (pathName != null) {
            path = new File(pathName);
            if (!path.isDirectory()) {
                try {
                    FileUtils.forceMkdir(path);
                    temporary = true;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        } else {
            path = tempPath();
            temporary = true;
        }
        dataPath0 = tempPath();
        dataPath1 = tempPath();
        dataPath2 = tempPath();
        // force reload
        tableDefinitionMap = null;
    }

    @Override
    public void clear() {
        try {
            if (temporary) {
                FileUtils.deleteDirectory(path);
            } else {
                FileUtils.cleanDirectory(path);
            }
            FileUtils.deleteDirectory(dataPath0);
            FileUtils.deleteDirectory(dataPath1);
            FileUtils.deleteDirectory(dataPath2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object get(@Nonnull String name) {
        if (name.equals(PROP_PATH)) {
            return path.getAbsolutePath();
        }
        return MetaService.super.get(name);
    }

    @Override
    public byte[] getTableKey(@Nonnull String tableName) {
        return new byte[0];
    }

    @Override
    public byte[] getIndexId(@Nonnull String tableName) {
        return new byte[0];
    }

    @Override
    public void createTable(@Nonnull String tableName, @Nonnull TableDefinition tableDefinition) {
        try {
            OutputStream os = new FileOutputStream(metaFile(tableName));
            tableDefinition.writeJson(os);
            // force reload
            tableDefinitionMap = null;
            Map<String, Location> partLocations = getPartLocations(tableName);
            for (Map.Entry<String, Location> entry : partLocations.entrySet()) {
                KvStoreInstance store = Services.KV_STORE.getInstance(entry.getValue().getPath());
                new PartInKvStore(
                    store.getKvBlock(tableName, entry.getKey()),
                    tableDefinition.getTupleSchema(),
                    tableDefinition.getKeyMapping()
                );
            }
        } catch (IOException e) {
            log.error("Failed to write table definition: {}", tableDefinition);
            throw new AssertionError("Failed to write table definition.");
        }
    }

    @Override
    public boolean dropTable(@Nonnull String tableName) {
        try {
            if (tableDefinitionMap.containsKey(tableName)) {
                FileUtils.forceDelete(metaFile(tableName));
                tableDefinitionMap.remove(tableName);
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public Map<String, TableDefinition> getTableDefinitions() {
        if (tableDefinitionMap != null) {
            return tableDefinitionMap;
        }
        if (path != null) {
            tableDefinitionMap = new LinkedHashMap<>();
            FileUtils.listFiles(path, new String[]{"json"}, true).forEach(f -> {
                try {
                    TableDefinition td = TableDefinition.readJson(new FileInputStream(f));
                    tableDefinitionMap.put(td.getName(), td);
                } catch (IOException e) {
                    throw new RuntimeException("Read table definition \"" + f.getName() + "\" failed.", e);
                }
            });
            return tableDefinitionMap;
        }
        throw new RuntimeException(this.getClass().getName() + " is not initialized.");
    }

    @Override
    public Location currentLocation() {
        return new Location("localhost", NetServiceConfiguration.INSTANCE.port(), dataPath0.getAbsolutePath());
    }

    @Override
    public Map<String, Location> getPartLocations(String name) {
        return ImmutableMap.of(
            "0", new Location("localhost", NetServiceConfiguration.INSTANCE.port(), dataPath0.getAbsolutePath()),
            "1", new Location("localhost", NetServiceConfiguration.INSTANCE.port(), dataPath1.getAbsolutePath()),
            "2", new Location("localhost", NetServiceConfiguration.INSTANCE.port(), dataPath2.getAbsolutePath())
        );
    }

    @Override
    public LocationGroup getLocationGroup(String name) {
        new LocationGroup(
            new Location("localhost", NetServiceConfiguration.INSTANCE.port(), dataPath0.getAbsolutePath()),
            Lists.newArrayList(
                new Location("localhost", NetServiceConfiguration.INSTANCE.port(), dataPath0.getAbsolutePath()),
                new Location("localhost", NetServiceConfiguration.INSTANCE.port(), dataPath0.getAbsolutePath()),
                new Location("localhost", NetServiceConfiguration.INSTANCE.port(), dataPath0.getAbsolutePath())));
        return null;
    }
}
