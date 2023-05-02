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

package io.dingodb.sdk.operation.context;

import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.sdk.common.Key;
import io.dingodb.sdk.common.Record;
import io.dingodb.sdk.operation.Column;
import io.dingodb.sdk.operation.Reader;
import io.dingodb.sdk.operation.Writer;
import io.dingodb.sdk.operation.filter.DingoFilter;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Context {

    public List<Key> startPrimaryKeys;
    private List<Key> endPrimaryKeys;
    private List<byte[]> startKeyBytes;
    private List<byte[]> endKeyBytes;
    private Column[] column;
    private DingoFilter filter;
    public TableDefinition definition;

    private List<Record> recordList;
    private boolean skippedWhenExisted;
    @Builder.Default
    private boolean useDefaultWhenNotExisted = false;
    @Builder.Default
    private boolean desc = true;

    public Map<String, Object> extArgs;

    private Reader reader;
    private Writer writer;
    private int timestamp;

    public Context definition(TableDefinition definition) {
        this.definition = definition;
        return this;
    }

    public Context reader(Reader reader) {
        this.reader = reader;
        return this;
    }

    public Reader reader() {
        return reader;
    }

    public Context writer(Writer writer) {
        this.writer = writer;
        return this;
    }

    public Writer writer() {
        return writer;
    }

    public Context timestamp(int timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public int timestamp() {
        return timestamp;
    }

    public Context startKey(List<byte[]> keys) {
        this.startKeyBytes = keys;
        return this;
    }

    public List<byte[]> startKey() throws IOException {
        if (startKeyBytes == null) {
            KeyValueCodec codec = keyValueCodec();
            return startPrimaryKeys.stream().map(x -> {
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
        return startKeyBytes;
    }

    public Context endKey(List<byte[]> keys) {
        this.endKeyBytes = keys;
        return this;
    }

    public List<byte[]> endKey() throws IOException {
        if (endKeyBytes == null) {
            if (endPrimaryKeys == null) {
                return null;
            }
            KeyValueCodec codec = keyValueCodec();
            return endPrimaryKeys.stream().map(x -> {
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
        return endKeyBytes;
    }

    public List<KeyValue> recordList() {
        KeyValueCodec codec = keyValueCodec();
        return recordList.stream().map(x -> {
            try {
                Object[] columnValues = x.getColumnValuesInOrder().toArray();
                return codec.encode(columnValues);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
    }

    public List<Key> getStartPrimaryKeys() {
        return startPrimaryKeys;
    }

    public List<Key> getEndPrimaryKeys() {
        return endPrimaryKeys;
    }

    public Column[] column() {
        return column;
    }

    public DingoFilter filter() {
        return filter;
    }

    public boolean isSkippedWhenExisted() {
        return skippedWhenExisted;
    }

    public boolean isUseDefaultWhenNotExisted() {
        return useDefaultWhenNotExisted;
    }

    public boolean desc() {
        return desc;
    }

    public KeyValueCodec keyValueCodec() {
        return null;// new DingoKeyValueCodec(definition.getDingoType(), definition.getKeyMapping());
    }
}
