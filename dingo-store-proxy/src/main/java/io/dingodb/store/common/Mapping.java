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

package io.dingodb.store.common;

import io.dingodb.common.CommonId;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.Range;
import io.dingodb.sdk.common.RangeWithOptions;
import io.dingodb.sdk.common.SDKCommonId;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.store.api.StoreInstance;

public final class Mapping {

    private Mapping() {
    }

    public static Column mapping(ColumnDefinition definition) {
        return io.dingodb.sdk.common.table.ColumnDefinition.builder()
            .name(definition.getName())
            .type(definition.getTypeName())
            .elementType(definition.getElementType())
            .precision(definition.getPrecision())
            .scale(definition.getScale())
            .nullable(definition.isNullable())
            .primary(definition.getPrimary())
            .defaultValue(definition.getDefaultValue())
            .build();
    }

    public static DingoCommonId mapping(CommonId commonId) {
        //return new io.dingodb.server.executor.common.DingoCommonId(commonId);
        return new SDKCommonId(DingoCommonId.Type.values()[commonId.type.code], commonId.domain, commonId.seq);
    }

    public static io.dingodb.sdk.common.KeyValue mapping(KeyValue keyValue) {
        return new io.dingodb.sdk.common.KeyValue(keyValue.getKey(), keyValue.getValue());
    }

    public static KeyValue mapping(io.dingodb.sdk.common.KeyValue keyValue) {
        return new KeyValue(keyValue.getKey(), keyValue.getValue());
    }

    public static RangeWithOptions mapping(StoreInstance.Range range) {
        return new RangeWithOptions(new Range(range.start, range.end), range.withStart, range.withEnd);
    }

}
