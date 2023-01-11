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

package io.dingodb.server.api;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.annotation.ApiDeclaration;
import io.dingodb.common.table.Index;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.Part;
import io.dingodb.server.protocol.meta.TablePart;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;

public interface TableApi {

    @ApiDeclaration
    default CompletableFuture<Boolean> createTable(
        CommonId id, TableDefinition tableDefinition, Map<CommonId, Location> mirrors
    ) {
        throw new UnsupportedOperationException();
    }

    @ApiDeclaration
    default CommonId getIndexId(CommonId tableId, String indexName) {
        throw new UnsupportedOperationException();
    }

    @ApiDeclaration
    default CompletableFuture<Void> deleteTable(CommonId id) {
        throw new UnsupportedOperationException();
    }

    @ApiDeclaration
    default TableDefinition getDefinition(CommonId tableId) {
        throw new UnsupportedOperationException();
    }

    @ApiDeclaration
    default List<TablePart> partitions(CommonId tableId) {
        throw new UnsupportedOperationException();
    }

    @ApiDeclaration
    default Map<CommonId, Location> mirrors(CommonId tableId) {
        throw new UnsupportedOperationException();
    }

    @ApiDeclaration
    default NavigableMap<ByteArrayUtils.ComparableByteArray, Part> getParts() {
        throw new UnsupportedOperationException();
    }

    @ApiDeclaration
    default CommonId createIndex(CommonId id, Index index) {
        throw new UnsupportedOperationException();
    }

    @ApiDeclaration
    default void deleteIndex(CommonId id, String name) {
        throw new UnsupportedOperationException();
    }

    @ApiDeclaration
    default boolean updateTableDefinition(CommonId id, TableDefinition tableDefinition) {
        throw new UnsupportedOperationException();
    }

}
