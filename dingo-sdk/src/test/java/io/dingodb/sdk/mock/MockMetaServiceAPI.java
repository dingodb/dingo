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

package io.dingodb.sdk.mock;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.Part;
import io.dingodb.server.api.MetaServiceApi;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class MockMetaServiceAPI implements MetaServiceApi {

    @Override
    public void createTable(@NonNull String tableName, @NonNull TableDefinition tableDefinition) {
    }

    @Override
    public boolean dropTable(@NonNull String tableName) {
        return false;
    }

    @Override
    public byte[] getTableKey(@NonNull String tableName) {
        return new byte[0];
    }

    @Override
    public CommonId getTableId(@NonNull String tableName) {
        return new CommonId(
            (byte) 'T',
            new byte[]{'D', 'T'},
            PrimitiveCodec.encodeInt(0),
            PrimitiveCodec.encodeInt(tableName.hashCode())
        );
    }

    @Override
    public byte[] getIndexId(@NonNull String tableName) {
        return new byte[0];
    }

    @Override
    public Map<String, TableDefinition> getTableDefinitions() {
        return null;
    }

    @Override
    public NavigableMap<ByteArrayUtils.ComparableByteArray, Part> getParts(String name) {
        return null;
    }

    @Override
    public List<Location> getDistributes(String name) {
        return null;
    }

    @Override
    public TableDefinition getTableDefinition(@NonNull CommonId commonId) {
        return null;
    }

    @Override
    public int registerUDF(CommonId id, String udfName, String function) {
        return 0;
    }

    @Override
    public boolean unregisterUDF(CommonId id, String udfName, int version) {
        return false;
    }

    @Override
    public String getUDF(CommonId id, String udfName) {
        return null;
    }

    @Override
    public String getUDF(CommonId id, String udfName, int version) {
        return null;
    }
}
