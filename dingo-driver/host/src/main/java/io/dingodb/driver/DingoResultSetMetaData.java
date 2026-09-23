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

package io.dingodb.driver;

import io.dingodb.calcite.meta.DingoColumnMetaData;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;

import java.sql.SQLException;
import java.util.List;

public class DingoResultSetMetaData extends AvaticaResultSetMetaData {
    private final List<ColumnMetaData> columns;

    public DingoResultSetMetaData(AvaticaStatement statement, Meta.Signature signature) {
        super(statement, null, signature);
        this.columns = signature.columns;
    }

    /** Returns null when the column has no expression-specific charset. */
    public String getColumnCharsetName(int column) throws SQLException {
        if (column < 1 || column > columns.size()) {
            throw new SQLException("Invalid column index: " + column);
        }
        ColumnMetaData metadata = columns.get(column - 1);
        return metadata instanceof DingoColumnMetaData ? ((DingoColumnMetaData) metadata).charsetName : null;
    }
}
