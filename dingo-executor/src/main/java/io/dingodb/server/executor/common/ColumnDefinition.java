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

public class ColumnDefinition implements Column {

    private final io.dingodb.common.table.ColumnDefinition columnDefinition;

    public ColumnDefinition(io.dingodb.common.table.ColumnDefinition columnDefinition) {
        this.columnDefinition = columnDefinition;
    }

    @Override
    public String getName() {
        return columnDefinition.getName();
    }

    @Override
    public String getType() {
        return columnDefinition.getTypeName();
    }

    @Override
    public String getElementType() {
        return columnDefinition.getElementType();
    }

    @Override
    public int getPrecision() {
        return columnDefinition.getPrecision();
    }

    @Override
    public int getScale() {
        return columnDefinition.getScale();
    }

    @Override
    public boolean isNullable() {
        return columnDefinition.isNullable();
    }

    @Override
    public int getPrimary() {
        return columnDefinition.getPrimary();
    }

    @Override
    public String getDefaultValue() {
        return columnDefinition.getDefaultValue();
    }
}
