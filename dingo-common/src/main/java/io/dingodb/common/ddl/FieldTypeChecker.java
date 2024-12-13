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

package io.dingodb.common.ddl;

import io.dingodb.common.exception.DingoSqlException;
import io.dingodb.common.session.Session;
import io.dingodb.common.session.SessionUtil;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.type.scalar.DateType;
import io.dingodb.common.type.scalar.TimeType;
import io.dingodb.common.type.scalar.TimestampType;

import java.sql.SQLException;
import java.util.List;

import static io.dingodb.common.mysql.DingoErrUtil.newStdErr;
import static io.dingodb.common.mysql.DingoErrUtil.newStdErrWithMsg;
import static io.dingodb.common.mysql.error.ErrorCode.ErrUnsupportedDDLOperation;
import static io.dingodb.common.mysql.error.ErrorCode.WarnDataTruncated;

public final class FieldTypeChecker {

    private FieldTypeChecker() {
    }

    public static DingoSqlException checkModifyTypeCompatible(
        String schemaName,
        String tableName,
        ColumnDefinition originColDef,
        ColumnDefinition toColDef
    ) {
        // if old col not has autoincrement and new col has autoincrement
        // unsupported
        if (!originColDef.isAutoIncrement() && toColDef.isAutoIncrement()) {
            return newStdErrWithMsg(
                "Unsupported modify column: can't set auto_increment", ErrUnsupportedDDLOperation);
        }
        if (originColDef.getTypeName().equalsIgnoreCase("ARRAY")
            || toColDef.getTypeName().equalsIgnoreCase("ARRAY")) {
            return newStdErr("Unsupported modify column: can't set array", ErrUnsupportedDDLOperation);
        }
        if (originColDef.getTypeName().equalsIgnoreCase(toColDef.getTypeName())) {
            if (originColDef.isNullable() && !toColDef.isNullable()) {
                return checkNullVal(schemaName, tableName, originColDef.getName());
            }
            return null;
        } else {
            if (originColDef.isNullable() && !toColDef.isNullable()) {
                return checkNullVal(schemaName, tableName, originColDef.getName());
            }
            boolean isTime = (toColDef.getType() instanceof TimeType)
                || (toColDef.getType() instanceof DateType) || (toColDef.getType() instanceof TimestampType);
            String originTypeName = originColDef.getTypeName();
            if ((originTypeName.equalsIgnoreCase("float")
                || originTypeName.equalsIgnoreCase("double")
                || originTypeName.equalsIgnoreCase("decimal")) && isTime) {
                return newStdErrWithMsg(
                "Unsupported modify column: change from original type %s to %s is currently unsupported yet",
                    ErrUnsupportedDDLOperation, originTypeName, toColDef.getTypeName());
            }
        }
        return null;
    }

    public static DingoSqlException checkNullVal(String schemaName, String tableName, String colName) {
        String sql = "select 1 from %s.%s where " + colName + " is null limit 1";
        Session session = SessionUtil.INSTANCE.getSession();
        try {
            sql = String.format(sql, schemaName, tableName);
            List<Object[]> res = session.executeQuery(sql);
            if (!res.isEmpty()) {
                return newStdErr(WarnDataTruncated, colName, res.size());
            }
            return null;
        } catch (SQLException e) {
            throw new DingoSqlException(e);
        } finally {
            session.destroy();
        }
    }

    public static boolean needChangeColumnData(ColumnDefinition originColDef, ColumnDefinition toColDef) {
        return originColDef.getTypeName().equalsIgnoreCase(toColDef.getTypeName());
    }

}
