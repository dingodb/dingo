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

package io.dingodb.calcite.meta;

import org.apache.calcite.avatica.ColumnMetaData;

public class DingoColumnMetaData extends ColumnMetaData {
    public final boolean hidden;

    public DingoColumnMetaData(
        int ordinal,
        boolean autoIncrement,
        boolean caseSensitive,
        boolean searchable,
        boolean currency,
        int nullable,
        boolean signed,
        int displaySize, String label, String columnName, String schemaName, int precision, int scale,
        String tableName, String catalogName, AvaticaType type, boolean readOnly, boolean writable,
        boolean definitelyWritable, String columnClassName,
        boolean hidden
    ) {
        super(ordinal, autoIncrement, caseSensitive, searchable, currency, nullable, signed, displaySize, label, columnName, schemaName, precision, scale, tableName, catalogName, type, readOnly, writable, definitelyWritable, columnClassName);
        this.hidden = hidden;
    }
}
