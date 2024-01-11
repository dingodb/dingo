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

package io.dingodb.calcite.operation;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.units.qual.C;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public interface DmlOperation extends Operation {
    boolean execute();

    default List<ColumnMetaData> columns(JavaTypeFactory typeFactory) {
        BasicSqlType basicSqlType = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT);
        ColumnMetaData.ScalarType type = ColumnMetaData.scalar(
            basicSqlType.getSqlTypeName().getJdbcOrdinal(),
            basicSqlType.getSqlTypeName().getName(),
            ColumnMetaData.Rep.of(typeFactory.getJavaClass(basicSqlType))
        );
        ColumnMetaData columnMetaData = new ColumnMetaData(0, false, true,
            false, false, 0, true, 19, "ROWCOUNT",
            "ROWCOUNT", null, 19, 0, null,
            null, type, true, false,
            false, "java.lang.Long");
        List<ColumnMetaData> columns = new ArrayList<>();
        columns.add(columnMetaData);
        return columns;
    }

    Iterator<Object[]> getIterator();

    default String getWarning() {
        return null;
    }
}
