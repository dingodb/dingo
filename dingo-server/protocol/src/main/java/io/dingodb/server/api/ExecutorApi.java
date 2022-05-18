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
import io.dingodb.common.store.KeyValue;
import io.dingodb.net.api.annotation.ApiDeclaration;

import java.util.List;

public interface ExecutorApi {
    @ApiDeclaration
    boolean upsertKeyValue(CommonId tableId, KeyValue row);

    @ApiDeclaration
    boolean upsertKeyValue(CommonId tableId, List<KeyValue> rows);

    @ApiDeclaration
    boolean upsertKeyValue(CommonId tableId, byte[] primaryKey, byte[] row);

    @ApiDeclaration
    public byte[] getValueByPrimaryKey(CommonId tableId, byte[] primaryKey);

    @ApiDeclaration
    public List<KeyValue> getKeyValueByPrimaryKeys(CommonId tableId, List<byte[]> primaryKeys);

    @ApiDeclaration
    public boolean delete(CommonId tableId, byte[] primaryKey);

    @ApiDeclaration
    public boolean delete(CommonId tableId, List<byte[]> primaryKeys);

    @ApiDeclaration
    public boolean deleteRange(CommonId tableId, byte[] startPrimaryKey, byte[] endPrimaryKey);

    @ApiDeclaration
    public List<KeyValue> getKeyValueByRange(CommonId tableId, byte[] startPrimaryKey, byte[] endPrimaryKey);
}
