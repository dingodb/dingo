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
import io.dingodb.common.operation.Column;
import io.dingodb.common.operation.context.BasicContext;
import io.dingodb.common.operation.context.OperationContext;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.sdk.common.Record;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public final class Converter {

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
            records = result4Store.getRecords().stream().filter(kv -> kv.getValue() != null).map(kv -> {
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
    @SuppressWarnings("unchecked")
    public static ContextForStore getStoreContext(ContextForClient inputContext,
                                                  KeyValueCodec codec,
                                                  TableDefinition definition) {
        List<byte[]> startKeyListInBytes = null;
        startKeyListInBytes = inputContext.getStartKeyList().stream().map(x -> {
            try {
                Object[] keys = x.getUserKey().toArray();
                if (keys.length != definition.getPrimaryKeyCount()) {
                    log.error("Inconsistent number of primary keys:{}", keys);
                }
                return codec.encodeKey(keys);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());

        List<byte[]> endKeyListInBytes = null;
        if (inputContext.getEndKeyList() != null) {
            endKeyListInBytes = inputContext.getEndKeyList().stream().map(x -> {
                try {
                    Object[] keys = x.getUserKey().toArray();
                    if (keys.length != definition.getPrimaryKeyCount()) {
                        log.error("Inconsistent number of primary keys:{}", keys);
                    }
                    return codec.encodeKey(keys);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList());
        }

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
                .peek(op -> op.operationContext.definition(definition))
                .map(ProtostuffCodec::write)
                .collect(Collectors.toList());
        }
        OperationContext context = null;
        if (inputContext.getFilter() != null) {
            context = new BasicContext();
            context.definition(definition);
            context.filter(inputContext.getFilter());
        }

        return ContextForStore.builder()
            .startKeyListInBytes(startKeyListInBytes)
            .endKeyListInBytes(endKeyListInBytes)
            .recordList(keyValueList)
            .operationListInBytes(operationListInBytes)
            .udfContext(inputContext.getUdfContext())
            .skippedWhenExisted(inputContext.isSkippedWhenExisted())
            .context(context)
            .build();
    }
}
