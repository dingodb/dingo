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

package io.dingodb.client.common;

import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.service.store.Coprocessor;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.List;

@Getter
@Builder
@EqualsAndHashCode
@AllArgsConstructor
public class VectorCoprocessor implements Coprocessor {

    private static final VectorSchemaWrapper DEFAULT_WRAPPER = VectorSchemaWrapper.builder().build();

    @Builder.Default
    private int schemaVersion = 1;
    @Builder.Default
    private VectorSchemaWrapper originalSchema = DEFAULT_WRAPPER;
    @Builder.Default
    private List<Integer> selection = Collections.emptyList();
    @Builder.Default
    private byte[] expression = ByteArrayUtils.EMPTY_BYTES;

    public VectorCoprocessor() {
    }

    @Getter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VectorSchemaWrapper implements SchemaWrapper {

        @Builder.Default
        private List<ColumnDefinition> schemas = Collections.emptyList();
        private long commonId;

        public List<Column> getSchemas() {
            return Collections.unmodifiableList(schemas);
        }
    }

    @Getter
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ColumnDefinition implements Column {

        public static final int DEFAULT_PRECISION = -1;
        public static final int DEFAULT_SCALE = Integer.MIN_VALUE;

        private String name;
        private String type;
        private String elementType;
        @Builder.Default
        private int precision = DEFAULT_PRECISION;
        @Builder.Default
        private int scale = DEFAULT_SCALE;
        @Builder.Default
        private boolean nullable = true;
        @Builder.Default
        private int primary = -1;
        private String defaultValue;
        private boolean isAutoIncrement;
        private int state;
        private String comment;

    }
}
