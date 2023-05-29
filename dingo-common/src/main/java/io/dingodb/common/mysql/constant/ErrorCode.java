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

package io.dingodb.common.mysql.constant;

public enum ErrorCode {
    ER_HASHCHK(1000, "HY000", "hashchk"),
    ER_NISAMCHK(1001, "HY000", "isamchk"),
    ER_NO(1002, "HY000", "NO"),
    ER_YES(1003, "HY000", "YES"),
    ER_CANT_CREATE_FILE(1004, "HY000", " Can't create file '%s' (errno: %d - %s)"),
    ER_CANT_CREATE_DB(1006, "HY000", "Can't create table '%s' (errno: %d)"),
    ER_ACCESS_DB_DENIED_ERROR(1044, "42000", "Access denied for user '%s'@'%s' to database '%s'"),
    ER_ACCESS_DENIED_ERROR(1045, "28000", "Access denied for user '%s'@'%s' (using password: %s)"),
    ER_NO_DATABASE_ERROR(1046, "3D000", "No database selected"),
    ER_BAD_DB_ERROR(1049, "42000", "Unknown database '%s'"),
    ER_COLUMN_ERROR(1054, "42S22", "Unknown column '%s' in '%s'"),
    ER_UNKNOWN_CHARACTER_SET(1115, "42000", "Unknown character set: '%s'"),
    ER_CREATE_TABLE(1142, "42000", "%s command denied to user '%s'@'%s' for table '%s'"),
    ER_NO_SUCH_TABLE(1146, "42S02", "Table '%s' doesn't exist"),
    ER_NOT_ALLOWED_COMMAND(1148, "42000", "The used command is not allowed with this MySQL version"),
    ER_ERROR_DURING_COMMIT(1180, "HY000", "Got error %d during COMMIT"),
    ER_UNKNOWN_ERROR(1105, "3D000", "Unknown error"),
    ER_UNKNOWN_VARIABLES(1193, "HY000", "Unknown system variable '%s'"),
    ER_IMMUTABLE_VARIABLES(1238, "HY000", "Variable '%s' is a read-only variable"),
    ER_PASSWORD_EXPIRE(1820, "HY000", "You must reset your password using ALTER USER statement before executing this statement."),

    ER_LOCK_ACCOUNT(3118, "HY000", "Access denied for user '%s'@'%s'. Account is locked.");

    public int code;
    public String sqlState;
    public String message;

    private ErrorCode(int code, String sqlState, String message) {
        this.code = code;
        this.sqlState = sqlState;
        this.message = message;
    }
}
