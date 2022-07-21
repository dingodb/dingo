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

package io.dingodb.sdk.operation;

import io.dingodb.common.codec.KeyValueCodec;
import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.sdk.common.Column;
import io.dingodb.sdk.common.Record;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public final class ConvertUtils {

    /**
     * convert Result from Storage to Result for client.
     * @param result4Store Result from Storage
     * @param codec codec about KeyValue
     * @param columnDefList column definition list
     * @return Result for client.
     */
    public static ResultForClient getResultCode(ResultForStore result4Store,
                                                KeyValueCodec codec,
                                                List<ColumnDefinition> columnDefList) {
        boolean status = result4Store.getStatus();
        String errorMessage = result4Store.getErrorMessage();
        List<Record> records = null;
        if (result4Store.getRecords() != null && !result4Store.getRecords().isEmpty()) {
            records = result4Store.getRecords().stream().map(kv -> {
                try {
                    List<Column> columnArray = new ArrayList<>();
                    Object[] columnValues = codec.decode(kv);
                    for (int i = 0; i < columnValues.length; i++) {
                        Column column = new Column(columnDefList.get(i).getName(), columnValues[i]);
                        columnArray.add(column);
                    }
                    return new Record(columnDefList, columnArray.toArray(new Column[columnArray.size()]));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList());
        }
        return new ResultForClient(status, errorMessage, records);
    }

    /**
     * convert context from client to storage.
     * @param inputContext  input client context
     * @param codec codec about KeyValue
     * @return ContextForStore.
     */
    public static ContextForStore getStoreContext(ContextForClient inputContext, KeyValueCodec codec) {
        List<byte[]> keyListInBytes = inputContext.getKeyList().stream().map(x -> {
            try {
                byte[] keyInBytes = codec.encodeKey(x.getUserKey().toArray());
                return keyInBytes;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());

        List<KeyValue> keyValueList = null;
        if (inputContext.getRecordList() != null) {
            keyValueList = inputContext.getRecordList().stream().map(x -> {
                try {
                    Object[] columnValues = x.getColumnValuesInOrder().toArray();
                    KeyValue keyValue = codec.encode(columnValues);
                    return keyValue;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList());
        }

        List<byte[]> operationListInBytes = null;
        if (inputContext.getOperationList() != null) {
            operationListInBytes = inputContext.getOperationList().stream()
                .map(ProtostuffCodec::write)
                .collect(Collectors.toList());
        }
        log.info("=>Key Count: {}, record Count:{}", keyListInBytes.size(), keyValueList.size());
        return new ContextForStore(keyListInBytes, keyValueList, operationListInBytes);
    }
}
