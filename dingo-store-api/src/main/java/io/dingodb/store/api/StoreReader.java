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

package io.dingodb.store.api;

import io.dingodb.common.store.KeyValue;
import io.dingodb.common.store.Row;
import lombok.experimental.Delegate;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public interface StoreReader {

    Getter getter();

    Scanner scanner();

    interface Getter {

        default Row getRow(byte[] primaryKey) {
            throw new UnsupportedOperationException();
        }

        default List<Row> getRows(List<byte[]> primaryKey) {
            throw new UnsupportedOperationException();
        }

        default byte[] getValue(byte[] primaryKey) {
            throw new UnsupportedOperationException();
        }

        default List<byte[]> getValues(List<byte[]> primaryKeys) {
            return getKeyValues(primaryKeys).stream().map(KeyValue::getValue).collect(Collectors.toList());
        }

        default List<KeyValue> getKeyValues(List<byte[]> primaryKeys) {
            throw new UnsupportedOperationException();
        }
    }

    interface Scanner {
        default Iterator<Row> scan() {
            throw new UnsupportedOperationException();
        }

        default Iterator<Row> scan(byte[] key) {
            return scan(key, key, true, true);
        }

        default Iterator<Row> scan(byte[] begin, byte[] end) {
            return scan(begin, end, true, false);
        }

        default Iterator<Row> scan(byte[] begin, byte[] end, boolean withBegin, boolean withEnd) {
            throw new UnsupportedOperationException();
        }

        default Iterator<KeyValue> keyValueScan() {
            throw new UnsupportedOperationException();
        }

        default Iterator<KeyValue> keyValueScan(byte[] key) {
            return keyValueScan(key, key, true, true);
        }

        default Iterator<KeyValue> keyValueScan(byte[] begin, byte[] end) {
            return keyValueScan(begin, end, true, false);
        }

        default Iterator<KeyValue> keyValueScan(byte[] begin, byte[] end, boolean withBegin, boolean withEnd) {
            throw new UnsupportedOperationException();
        }
    }

}
