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

import io.dingodb.common.codec.CodeTag;
import io.dingodb.common.codec.DingoKeyValueCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.mpu.instruction.Instruction;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Accessors(chain = true, fluent = true)
public class Writer implements io.dingodb.mpu.storage.Writer {
    @Getter
    private final Instruction instruction;
    private DingoKeyValueCodec codec;
    private TableDefinition definition;
    protected List<Object[]> records = new ArrayList<>();
    private final Reader reader;

    public Writer(Instruction instruction, TableDefinition definition, Reader reader) {
        this.instruction = instruction;
        this.definition = definition;
        this.codec = definition.createCodec();
        this.reader = reader;
    }

    @Override
    public int count() {
        return 0;
    }

    @Override
    public void set(byte[] key, byte[] value) {
        if (key != null && key.length > 0 && key[0] == CodeTag.UNFINISHFALG) {
            log.info("Writer set, not real set.");
            return;
        }
        if (reader.get(key) != null) {
            throw new RuntimeException("Unsupported operation!");
        }

        try {
            Object[] kv = this.codec.decode(new KeyValue(key, value));
            records.add(kv);
            log.info("Writer set, records size: {}.", records.size());
        } catch (IOException ioException) {
            throw new RuntimeException(ioException);
        }
        return;
    }

    @Override
    public void erase(byte[] key) {
        log.info("Writer erase.");
    }

    // [start, end)
    @Override
    public void erase(byte[] begin, byte[] end) {
        log.info("Writer erase from begin to end.");
    }

    public void close() {
        reader.close();
        return;
    }

    public String[] getColumnNames() {
        List<ColumnDefinition> columns = definition.getColumns();
        String[] columnNames = new String[columns.size()];
        int i = 0;
        for (ColumnDefinition column : columns) {
            columnNames[i] = column.getName();
            i++;
        }
        return columnNames;
    }

    public String[][] getColumnData() {
        List<ColumnDefinition>  columns = definition.getColumns();
        if (records.size() <= 0) {
            return null;
        }
        int length = records.get(0).length;
        if (columns.size() != length) {
            throw new RuntimeException("Writer getColumnData, size not match " + columns.size() + " | " + length);
        }
        log.info("Writer getColumnData column count: {}, row count: {}.", length, records.size());
        String[][] result = new String[length][records.size()]; // [column][rows]
        for (int i = 0; i < records.size(); i++) {
            Object[] objs = records.get(i);
            for (int j = 0; j < objs.length; j++) {
                DingoType type = columns.get(j).getType();
                String str = TypeConvert.DingoDataToString(type, objs[j]);
                result[j][i] = str;
            }
        }
        return result;
    }
}
