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

package io.dingodb.client.operation;

import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.sdk.common.table.Column;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Collections;
import java.util.List;

@Getter
@EqualsAndHashCode
@AllArgsConstructor
public class Coprocessor implements io.dingodb.sdk.service.store.Coprocessor {

    public final List<io.dingodb.sdk.service.store.AggregationOperator> aggregations;
    public final SchemaWrapper originalSchema;
    public final SchemaWrapper resultSchema;
    public final List<Integer> groupBy;

    @Override
    public int getSchemaVersion() {
        return 1;
    }

    @Override
    public List<Integer> getSelection() {
        return Collections.emptyList();
    }

    @Override
    public byte[] getExpression() {
        return ByteArrayUtils.EMPTY_BYTES;
    }

    @Getter
    @Builder
    @AllArgsConstructor
    public static class SchemaWrapper implements io.dingodb.sdk.service.store.Coprocessor.SchemaWrapper {

        private final long commonId;
        private final List<Column> schemas;

    }

    @Getter
    @EqualsAndHashCode
    @AllArgsConstructor
    public static class AggregationOperator implements io.dingodb.sdk.service.store.AggregationOperator {

        public final AggregationType operation;
        public final int indexOfColumn;

    }
}
