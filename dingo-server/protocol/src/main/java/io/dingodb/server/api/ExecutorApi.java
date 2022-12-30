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
import io.dingodb.net.Channel;

import java.util.List;
import java.util.concurrent.Future;

public interface ExecutorApi {

    @ApiDeclaration
    default boolean exist(CommonId tableId, byte[] primaryKey) {
        throw new UnsupportedOperationException();
    }

    default boolean upsertKeyValue(CommonId tableId, KeyValue row) {
        return upsertKeyValue(null, null, tableId, row);
    }

    default boolean delete(CommonId tableId, byte[] primaryKey) {
        return delete(null, null, tableId, primaryKey);
    }

    @ApiDeclaration
    @TransferArgsCodecAnnotation(name = "UpsertKeyValueCodeCUsingKeyValue")
    public boolean upsertKeyValue(Channel channel, CommonId schema, CommonId tableId, KeyValue row);

    @ApiDeclaration
    @TransferArgsCodecAnnotation(name = "UpsertKeyValueCodeCUsingListKeyValue")
    public boolean upsertKeyValue(Channel channel, CommonId schema, CommonId tableId, List<KeyValue> rows);

    @ApiDeclaration
    @TransferArgsCodecAnnotation(name = "UpsertKeyValueCodeCUsingByteArray")
    public boolean upsertKeyValue(Channel channel, CommonId schema, CommonId tableId, byte[] primaryKey, byte[] row);

    @ApiDeclaration
    public byte[] getValueByPrimaryKey(Channel channel, CommonId schema, CommonId tableId, byte[] primaryKey);

    @ApiDeclaration
    public List<KeyValue> getKeyValueByPrimaryKeys(Channel channel, CommonId schema, CommonId tableId,
                                            List<byte[]> primaryKeys);

    @ApiDeclaration
    public boolean delete(Channel channel, CommonId schema, CommonId tableId, byte[] primaryKey);

    @ApiDeclaration
    public boolean delete(Channel channel, CommonId schema, CommonId tableId, List<byte[]> primaryKeys);

    @ApiDeclaration
    public boolean deleteRange(Channel channel, CommonId schema, CommonId tableId,
                        byte[] startPrimaryKey, byte[] endPrimaryKey);

    @ApiDeclaration
    public List<KeyValue> getKeyValueByRange(Channel channel, CommonId schema, CommonId tableId,
                                      byte[] startPrimaryKey, byte[] endPrimaryKey);

    @ApiDeclaration
    public List<KeyValue> getKeyValueByKeyPrefix(Channel channel, CommonId schema, CommonId tableId,
                                        byte[] keyPrefix);

    @ApiDeclaration
    public List<KeyValue> getAllKeyValue(Channel channel, CommonId schema, CommonId tableId);

    @ApiDeclaration
    public Future<Object> operator(CommonId tableId, List<byte[]> startPrimaryKey, List<byte[]> endPrimaryKey,
                                    byte[] op, boolean readOnly);

    @ApiDeclaration
    default KeyValue udfGet(CommonId tableId, byte[] primaryKey, String udfName, String functionName) {
        return udfGet(tableId, primaryKey, udfName, functionName, 0);
    }

    @ApiDeclaration
    default  KeyValue udfGet(CommonId tableId, byte[] primaryKey, String udfName, String functionName, int version) {
        throw new UnsupportedOperationException();
    }

    @ApiDeclaration
    default boolean udfUpdate(CommonId tableId, byte[] primaryKey, String udfName, String functionName) {
        return udfUpdate(tableId, primaryKey, udfName, functionName, 0);
    }

    @ApiDeclaration
    default boolean udfUpdate(CommonId tableId, byte[] primaryKey, String udfName, String functionName, int version) {
        throw new UnsupportedOperationException();
    }

    @ApiDeclaration
    default boolean insert(CommonId tableId, Object[] row) {
        throw new UnsupportedOperationException();
    }

    @ApiDeclaration
    default boolean update(CommonId tableId, Object[] row) {
        throw new UnsupportedOperationException();
    }

    @ApiDeclaration
    default boolean delete(CommonId tableId, Object[] row) {
        throw new UnsupportedOperationException();
    }

    @ApiDeclaration
    default List<Object[]> select(CommonId tableId, Object[] row, boolean[] hasData) {
        throw new UnsupportedOperationException();
    }
}
