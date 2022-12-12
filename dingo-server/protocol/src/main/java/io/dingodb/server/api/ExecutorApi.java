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
import io.dingodb.common.annotation.ApiDeclaration;
import io.dingodb.common.codec.annotation.TransferArgsCodecAnnotation;
import io.dingodb.common.store.KeyValue;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;

public interface ExecutorApi {

    @ApiDeclaration
    boolean exist(CommonId tableId, byte[] primaryKey);

    @ApiDeclaration
    @TransferArgsCodecAnnotation(name = "UpsertKeyValueCodeCUsingKeyValue")
    boolean upsertKeyValue(CommonId tableId, KeyValue row);

    @ApiDeclaration
    @TransferArgsCodecAnnotation(name = "UpsertKeyValueCodeCUsingListKeyValue")
    boolean upsertKeyValue(CommonId tableId, List<KeyValue> rows);

    @ApiDeclaration
    @TransferArgsCodecAnnotation(name = "UpsertKeyValueCodeCUsingByteArray")
    boolean upsertKeyValue(CommonId tableId, byte[] primaryKey, byte[] row);

    @ApiDeclaration
    byte[] getValueByPrimaryKey(CommonId tableId, byte[] primaryKey);

    @ApiDeclaration
    List<KeyValue> getKeyValueByPrimaryKeys(CommonId tableId, List<byte[]> primaryKeys);

    @ApiDeclaration
    boolean delete(CommonId tableId, byte[] primaryKey);

    @ApiDeclaration
    boolean delete(CommonId tableId, List<byte[]> primaryKeys);

    @ApiDeclaration
    boolean deleteRange(CommonId tableId, byte[] startPrimaryKey, byte[] endPrimaryKey);

    @ApiDeclaration
    List<KeyValue> getKeyValueByRange(CommonId tableId, byte[] startPrimaryKey, byte[] endPrimaryKey);

    @ApiDeclaration
    Future<Object> operator(CommonId tableId, List<byte[]> startPrimaryKey, List<byte[]> endPrimaryKey, byte[] op);

    @ApiDeclaration
    default KeyValue udfGet(CommonId tableId, byte[] primaryKey, String udfName, String functionName) {
        return udfGet(tableId, primaryKey, udfName, functionName, 0);
    }

    @ApiDeclaration
    KeyValue udfGet(CommonId tableId, byte[] primaryKey, String udfName, String functionName, int version);

    @ApiDeclaration
    default boolean udfUpdate(CommonId tableId, byte[] primaryKey, String udfName, String functionName) {
        return udfUpdate(tableId, primaryKey, udfName, functionName, 0);
    }

    @ApiDeclaration
    boolean udfUpdate(CommonId tableId, byte[] primaryKey, String udfName, String functionName, int version);

    @ApiDeclaration
    boolean updateTableDefinitionVersion(CommonId tableId, int version);

    @ApiDeclaration
    Iterator<KeyValue> getAllFinishedRecord(CommonId tableId);

    @ApiDeclaration
    Iterator<KeyValue> getAllUnfinishedRecord(CommonId tableId);

    @ApiDeclaration
    boolean insertUnfinishedRecord(CommonId tableId, KeyValue record);

    @ApiDeclaration
    boolean deleteUnfinishedRecord(CommonId tableId, byte[] key);

    @ApiDeclaration
    boolean insertFinishedRecord(CommonId tableId, byte[] key, int tableDefinitionVersion);

    @ApiDeclaration
    byte[] getUnfinishedKey(byte[] key);

    @ApiDeclaration
    byte[] getFinishedKey(byte[] key);

    @ApiDeclaration
    boolean insertIndex(CommonId tableId, KeyValue record);

    @ApiDeclaration
    KeyValue getRecord(CommonId tableId, byte[] key);

    @ApiDeclaration
    void deleteIndex(CommonId tableId, KeyValue keyValue);

    @ApiDeclaration
    void deleteFinishedRecord(CommonId tableId, byte[] key);

    @ApiDeclaration
    Iterator<KeyValue> getFinishedRecord(CommonId tableId, List<KeyValue> records);

    @ApiDeclaration
    void insertDeleteKey(CommonId tableId, KeyValue keyValue);

    @ApiDeclaration
    void deleteDeleteKey(CommonId tableId, byte[] key);

    @ApiDeclaration
    Iterator<KeyValue> getAllDeleteRecord(CommonId tableId);
}
