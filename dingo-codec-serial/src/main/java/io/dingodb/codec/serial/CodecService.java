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

package io.dingodb.codec.serial;

import com.google.auto.service.AutoService;
import io.dingodb.codec.CodecServiceProvider;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;

import java.util.List;

public class CodecService implements io.dingodb.codec.CodecService {

    public static final CodecService INSTANCE = new CodecService();

    @AutoService(CodecServiceProvider.class)
    public static class Provider implements CodecServiceProvider {
        @Override
        public CodecService get() {
            return INSTANCE;
        }
    }

    @Override
    public KeyValueCodec createKeyValueCodec(
        int codecVersion, int version, CommonId id, DingoType type, TupleMapping keyMapping
    ) {
        return createKeyValueCodec(codecVersion, id, type, keyMapping);
    }

    @Override
    public KeyValueCodec createKeyValueCodec(
        int codecVersion, int version, CommonId id, List<ColumnDefinition> columns
    ) {
        return createKeyValueCodec(codecVersion, id, columns);
    }

    @Override
    public KeyValueCodec createKeyValueCodec(int codecVersion, CommonId id, DingoType type, TupleMapping keyMapping) {
        return new DingoKeyValueCodec(type, keyMapping);
    }

    @Override
    public KeyValueCodec createKeyValueCodec(int codecVersion, CommonId id, List<ColumnDefinition> columns) {
        TableDefinition tableDefinition = new TableDefinition("");
        tableDefinition.setColumns(columns);
        return createKeyValueCodec(codecVersion, id, tableDefinition.getDingoType(), tableDefinition.getKeyMapping());
    }

    @Override
    public KeyValueCodec createKeyValueCodec(CommonId id, TableDefinition tableDefinition) {
        return createKeyValueCodec(
            tableDefinition.getCodecVersion(), id, tableDefinition.getDingoType(), tableDefinition.getKeyMapping());
    }
}
