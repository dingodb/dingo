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

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import javax.annotation.Nonnull;

public class MockMetaServiceAPI implements MetaServiceApi {

    @Override
    public void createTable(@Nonnull String tableName, @Nonnull TableDefinition tableDefinition) {
        return ;
    }

    @Override
    public boolean dropTable(@Nonnull String tableName) {
        return false;
    }

    @Override
    public byte[] getTableKey(@Nonnull String tableName) {
        return new byte[0];
    }

    @Override
    public CommonId getTableId(@Nonnull String tableName) {
        return new CommonId(
            (byte) 'T',
            new byte[] {'D', 'T'},
            PrimitiveCodec.encodeInt(0),
            PrimitiveCodec.encodeInt(tableName.hashCode())
        );
    }

    @Override
    public byte[] getIndexId(@Nonnull String tableName) {
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
    public TableDefinition getTableDefinition(@Nonnull CommonId commonId) {
        return null;
    }
}
