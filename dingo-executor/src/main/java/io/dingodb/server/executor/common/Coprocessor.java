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

package io.dingodb.server.executor.common;

import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.service.store.AggregationOperator;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;
import java.util.stream.Collectors;

@EqualsAndHashCode
public class Coprocessor implements io.dingodb.sdk.service.store.Coprocessor {

    private final io.dingodb.common.Coprocessor coprocessor;

    public Coprocessor(io.dingodb.common.Coprocessor coprocessor) {
        this.coprocessor = coprocessor;
    }

    @Override
    public int getSchemaVersion() {
        return coprocessor.getSchemaVersion();
    }

    @Override
    public io.dingodb.sdk.service.store.Coprocessor.SchemaWrapper getOriginalSchema() {
        return new SchemaWrapper(coprocessor.getOriginalSchema());
    }

    @Override
    public SchemaWrapper getResultSchema() {
        return new SchemaWrapper(coprocessor.getResultSchema());
    }

    @Override
    public List<Integer> getSelection() {
        return coprocessor.getSelection();
    }

    @Override
    public byte[] getExpression() {
        return coprocessor.getExpression();
    }

    @Override
    public List<Integer> getGroupBy() {
        return coprocessor.getGroupBy();
    }

    @Override
    public List<AggregationOperator> getAggregations() {
        return coprocessor.getAggregations().stream()
            .map(io.dingodb.server.executor.common.AggregationOperator::new)
            .collect(Collectors.toList());
    }

    @Getter
    @Builder
    @AllArgsConstructor
    public static class SchemaWrapper implements io.dingodb.sdk.service.store.Coprocessor.SchemaWrapper {

        private final io.dingodb.common.SchemaWrapper schemaWrapper;

        @Override
        public List<Column> getSchemas() {
            return schemaWrapper.getSchemas().stream().map(ColumnDefinition::new).collect(Collectors.toList());
        }

        @Override
        public long getCommonId() {
            return schemaWrapper.getCommonId();
        }
    }

}
