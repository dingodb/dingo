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

package io.dingodb.server.executor.store.column;

import io.dingodb.common.codec.DingoKeyValueCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.strorage.column.ColumnBlock;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class Reader implements io.dingodb.mpu.storage.Reader {
    private TableDefinition definition;
    private DingoKeyValueCodec codec;
    private final String table;

    public Reader(TableDefinition definition, final String table) {
        this.definition = definition;
        this.table = table;
        this.codec = definition.createCodec();
    }
    @Override
    public Iterator scan(byte[] startKey, byte[] endKey, boolean withStart, boolean withEnd) {
        return new Iterator(this.definition, this.table, startKey, endKey, withStart, withEnd);
    }

    @Override
    public long count(byte[] start, byte[] end) {
        throw new RuntimeException("Count not support!");
    }

    @Override
    public byte[] get(byte[] key) {
        Iterator iterator = this.scan(key, key, true, true);
        byte[] result = null;
        boolean first = false;
        while (iterator.hasNext()) {
            if (!first) {
                result = iterator.next().getValue();
                first = true;
            }
        }
        return result;
    }

    @Override
    public List<KeyValue> get(List<byte[]> keys) {
        List<KeyValue> entries = new ArrayList<>(keys.size());
        return entries;
    }

    @Override
    public void close() {
    }

    @Slf4j
    static class Iterator implements java.util.Iterator<KeyValue> {
        ColumnBlock block;
        java.util.Iterator<String[]> cache;
        DingoKeyValueCodec codec;

        final String database = "default";
        final String table;
        final String sessionID = UUID.randomUUID().toString();
        final String[] columnNames;
        final Integer[] columnTypes;
        final DingoType[] dingoTypes;
        String primaryKey;
        DingoType primaryType;

        Iterator(TableDefinition definition, final String table, byte[] start, byte[] end, boolean withStart,
                 boolean withEnd) {
            this.table = table;
            this.codec = definition.createCodec();
            block = new ColumnBlock();

            int size = definition.getColumns().size();
            columnNames = new String[size];
            columnTypes = new Integer[size];
            dingoTypes = new DingoType[size];
            int i = 0;
            for (ColumnDefinition column : definition.getColumns()) {
                columnNames[i] = column.getName();
                dingoTypes[i] = column.getType();
                int type = TypeConvert.DingoTypeToCKType(column.getType());
                columnTypes[i] = type;
                if (primaryKey == null && column.isPrimary()) {
                    primaryKey = column.getName();
                    primaryType = column.getType();
                }
                i++;
            }

            int ret = 0;
            if (start == null || end == null) {
                ret = block.blockSelectTableBegin(sessionID, database, table, primaryKey, columnNames);
            } else {
                try {
                    String startKey = TypeConvert.DingoDataToString(primaryType, this.codec.decodeKey(start)[0]);
                    String endKey = TypeConvert.DingoDataToString(primaryType, this.codec.decodeKey(end)[0]);
                    ret = block.blockSelectTableBegin(sessionID, database, this.table, primaryKey, columnNames,
                        startKey, endKey);
                } catch (IOException ioException) {
                    throw new RuntimeException(ioException);
                }
            }
            if (ret < 0) {
                throw new RuntimeException("blockSelectTableBegin, ret: " + ret);
            }
            this.cache = getBlockData(block, columnTypes).iterator();
        }

        @Override
        public boolean hasNext() {
            if (cache.hasNext()) {
                return true;
            }
            if (block.getRows() <= 0) {
                return false;
            }

            block.clearBlock();
            int ret = block.blockSelectTableNext(sessionID);
            if (ret < 0) {
                throw new RuntimeException("get block error, sessionID: " + sessionID);
            }
            this.cache = getBlockData(this.block, columnTypes).iterator();
            return this.cache.hasNext();
        }

        @Override
        public KeyValue next() {
            String[] row = cache.next();
            if (row.length != dingoTypes.length) {
                throw new RuntimeException("row count not match column type count: " + row.length + " | "
                    + columnTypes.length);
            }

            Object[] objs = new Object[row.length];
            for (int i = 0; i < row.length; i++) {
                Object obj = TypeConvert.StringToDingoData(dingoTypes[i], row[i]);
                objs[i] = obj;
            }
            try {
                return this.codec.encode(objs);
            } catch (IOException ioException) {
                throw new RuntimeException(ioException);
            }
        }

        // close iterator
        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            block.close();
        }

        private static List<String[]> getBlockData(ColumnBlock block, Integer[] types) {
            int columnCount = (int)block.getColumns();
            int rows = (int)block.getRows();
            String[][] result = new String[rows][columnCount];
            for(int i = 0; i < columnCount; i++) {
                String[] strings = block.getColumnData(i, types[i]);
                if (rows != strings.length) {
                    throw new RuntimeException("rows not match: " + rows + " | " + strings.length);
                }
                for (int j = 0; j < rows; j++) {
                    result[j][i] = strings[j];
                }
            }
            return Arrays.asList(result);
        }
    }
}
