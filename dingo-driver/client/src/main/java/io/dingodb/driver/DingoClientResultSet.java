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

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.util.AbstractCursor;
import org.apache.calcite.avatica.util.Cursor;
import org.apache.calcite.avatica.util.DingoAccessor;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.TimeZone;

public class DingoClientResultSet extends AvaticaResultSet {

    public DingoClientResultSet(
        AvaticaStatement statement,
        QueryState state,
        Meta.Signature signature,
        ResultSetMetaData resultSetMetaData,
        TimeZone timeZone,
        Meta.Frame firstFrame
    ) throws SQLException {
        super(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        try {
            Object object;
            switch (columnMetaDataList.get(columnIndex - 1).type.id) {
                case Types.FLOAT:
                    object = accessorList.get(columnIndex - 1).getObject();
                    break;
                default:
                    object = super.getObject(columnIndex);
                    break;
            }
            return object;
        } catch (IndexOutOfBoundsException e) {
            throw AvaticaConnection.HELPER.createException(
                "invalid column ordinal: " + columnIndex);
        }
    }

    @Override
    protected AvaticaResultSet execute() throws SQLException {
        super.execute();
        for (int i = 0; i < this.columnMetaDataList.size(); i++) {
            ColumnMetaData columnMetaData = columnMetaDataList.get(i);
            if (columnMetaData.type.id == Types.FLOAT) {
                this.accessorList.set(i, new DingoAccessor.FloatAccessor((AbstractCursor) cursor, i));
            }

            if (columnMetaData.type.id == Types.ARRAY) {
                ColumnMetaData.ArrayType arrayType = (ColumnMetaData.ArrayType) columnMetaData.type;
                if (arrayType.getComponent().id == Types.FLOAT
                    || arrayType.getComponent().id == Types.TIME
                    || arrayType.getComponent().id == Types.DATE
                ) {
                    this.accessorList.set(i, new DingoAccessor.ArrayAccessor(
                            (AbstractCursor) cursor, i, this.localCalendar, arrayType.getComponent()
                    ));
                }
            }
        }
        return this;
    }

    @Override
    public AvaticaResultSet execute2(Cursor cursor, List<ColumnMetaData> columnMetaDataList) {
        super.execute2(cursor, columnMetaDataList);
        for (int i = 0; i < this.columnMetaDataList.size(); i++) {
            ColumnMetaData columnMetaData = columnMetaDataList.get(i);
            if (columnMetaData.type.id == Types.FLOAT) {
                this.accessorList.set(i, new DingoAccessor.FloatAccessor((AbstractCursor) cursor, i));
            }
            if (columnMetaData.type.id == Types.ARRAY) {
                ColumnMetaData.ArrayType arrayType = (ColumnMetaData.ArrayType) columnMetaData.type;
                if (arrayType.getComponent().id == Types.FLOAT
                    || arrayType.getComponent().id == Types.TIME
                    || arrayType.getComponent().id == Types.DATE) {
                    this.accessorList.set(i, new DingoAccessor.ArrayAccessor(
                        (AbstractCursor) cursor, i, this.localCalendar, arrayType.getComponent()
                    ));
                }
            }
        }
        return this;
    }
}
