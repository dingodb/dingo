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
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.Part;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Slf4j
public class MetaTestService implements MetaService {
    public static final String SCHEMA_NAME = "TEST";

    public static final MetaTestService INSTANCE = new MetaTestService();
    private final Map<String, TableDefinition> tableDefinitionMap = new LinkedHashMap<>();

    @Override
    public String getName() {
        return SCHEMA_NAME;
    }

    @Override
    public void init(@Nullable Map<String, Object> props) {
    }

    @Override
    public void clear() {
        tableDefinitionMap.keySet().stream()
            .map(name -> new CommonId(
                (byte) 'T',
                new byte[]{'D', 'T'},
                PrimitiveCodec.encodeInt(0),
                PrimitiveCodec.encodeInt(name.hashCode())
            )).forEach(StoreTestServiceProvider.STORE_SERVICE::deleteInstance);
        tableDefinitionMap.clear();
    }

    @Override
    public void createTable(@Nonnull @NonNull String tableName, @Nonnull @NonNull TableDefinition tableDefinition) {
        tableDefinitionMap.put(tableName, tableDefinition);
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
    public byte[] getTableKey(@Nonnull @NonNull String tableName) {
        return tableName.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public CommonId getTableId(@Nonnull @NonNull String tableName) {
        return new CommonId(
            (byte) 'T',
            new byte[]{'D', 'T'},
            PrimitiveCodec.encodeInt(0),
            PrimitiveCodec.encodeInt(tableName.hashCode())
        );
    }

    @Override
    public byte[] getIndexId(@Nonnull @NonNull String tableName) {
        return new byte[0];
    }

    @Override
    public Map<String, TableDefinition> getTableDefinitions() {
        return tableDefinitionMap;
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
        /*
        startKey = endKey;
        endKey = PrimitiveCodec.encodeVarInt(6);
        result.put(new ComparableByteArray(startKey), new Part(
            startKey,
            new FakeLocation(1),
            Collections.singletonList(new FakeLocation(1)),
            startKey,
            endKey
        ));
        startKey = endKey;
        endKey = ByteArrayUtils.MAX_BYTES;
        result.put(new ComparableByteArray(startKey), new Part(
            startKey,
            new FakeLocation(2),
            Collections.singletonList(new FakeLocation(2)),
            startKey,
            endKey
        ));
         */
        return result;
    }

    @Override
    public List<Location> getDistributes(String name) {
        return Collections.singletonList(new FakeLocation(1));
    }

    @Override
    public Location currentLocation() {
        return new FakeLocation(0);
    }
}
