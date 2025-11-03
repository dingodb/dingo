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

package io.dingodb.common.mysql;

import java.util.HashSet;
import java.util.Set;

import static io.dingodb.common.mysql.SQLModeFlags.MODE_ANSI_QUOTES;
import static io.dingodb.common.mysql.SQLModeFlags.MODE_ERROR_FOR_DIVISION_BY_ZERO;
import static io.dingodb.common.mysql.SQLModeFlags.MODE_HIGH_NOT_PRECEDENCE;
import static io.dingodb.common.mysql.SQLModeFlags.MODE_IGNORE_SPACE;
import static io.dingodb.common.mysql.SQLModeFlags.MODE_INVALID_DATES;
import static io.dingodb.common.mysql.SQLModeFlags.MODE_NO_AUTO_CREATE_USER;
import static io.dingodb.common.mysql.SQLModeFlags.MODE_NO_AUTO_VALUE_ON_ZERO;
import static io.dingodb.common.mysql.SQLModeFlags.MODE_NO_BACKSLASH_ESCAPES;
import static io.dingodb.common.mysql.SQLModeFlags.MODE_NO_DIR_IN_CREATE;
import static io.dingodb.common.mysql.SQLModeFlags.MODE_NO_ENGINE_SUBSTITUTION;
import static io.dingodb.common.mysql.SQLModeFlags.MODE_NO_FIELD_OPTIONS;
import static io.dingodb.common.mysql.SQLModeFlags.MODE_NO_KEY_OPTIONS;
import static io.dingodb.common.mysql.SQLModeFlags.MODE_NO_TABLE_OPTIONS;
import static io.dingodb.common.mysql.SQLModeFlags.MODE_NO_UNSIGNED_SUBTRACTION;
import static io.dingodb.common.mysql.SQLModeFlags.MODE_NO_ZERO_DATE;
import static io.dingodb.common.mysql.SQLModeFlags.MODE_NO_ZERO_IN_DATE;
import static io.dingodb.common.mysql.SQLModeFlags.MODE_ONLY_FULL_GROUP_BY;
import static io.dingodb.common.mysql.SQLModeFlags.MODE_PAD_CHAR_TO_FULL_LENGTH;
import static io.dingodb.common.mysql.SQLModeFlags.MODE_PIPES_AS_CONCAT;
import static io.dingodb.common.mysql.SQLModeFlags.MODE_REAL_AS_FLOAT;
import static io.dingodb.common.mysql.SQLModeFlags.MODE_STRICT_ALL_TABLES;
import static io.dingodb.common.mysql.SQLModeFlags.MODE_STRICT_TRANS_TABLES;

public enum SQLMode {
    ALLOW_INVALID_DATES(MODE_INVALID_DATES),
    ANSI_QUOTES(MODE_ANSI_QUOTES),
    ERROR_FOR_DIVISION_BY_ZERO(MODE_ERROR_FOR_DIVISION_BY_ZERO),
    HIGH_NOT_PRECEDENCE(MODE_HIGH_NOT_PRECEDENCE),
    IGNORE_SPACE(MODE_IGNORE_SPACE),
    NO_AUTO_CREATE_USER(MODE_NO_AUTO_CREATE_USER),
    NO_AUTO_VALUE_ON_ZERO(MODE_NO_AUTO_VALUE_ON_ZERO),
    NO_BACKSLASH_ESCAPES(MODE_NO_BACKSLASH_ESCAPES),
    NO_DIR_IN_CREATE(MODE_NO_DIR_IN_CREATE),
    NO_ENGINE_SUBSTITUTION(MODE_NO_ENGINE_SUBSTITUTION),
    NO_FIELD_OPTIONS(MODE_NO_FIELD_OPTIONS),
    NO_KEY_OPTIONS(MODE_NO_KEY_OPTIONS),
    NO_TABLE_OPTIONS(MODE_NO_TABLE_OPTIONS),
    NO_UNSIGNED_SUBTRACTION(MODE_NO_UNSIGNED_SUBTRACTION),
    NO_ZERO_DATE(MODE_NO_ZERO_DATE),
    NO_ZERO_IN_DATE(MODE_NO_ZERO_IN_DATE),
    ONLY_FULL_GROUP_BY(MODE_ONLY_FULL_GROUP_BY),
    PAD_CHAR_TO_FULL_LENGTH(MODE_PAD_CHAR_TO_FULL_LENGTH),
    PIPES_AS_CONCAT(MODE_PIPES_AS_CONCAT),
    REAL_AS_FLOAT(MODE_REAL_AS_FLOAT),
    STRICT_ALL_TABLES(MODE_STRICT_ALL_TABLES),
    STRICT_TRANS_TABLES(MODE_STRICT_TRANS_TABLES);

    private final long sqlModeFlag;

    SQLMode(long sqlModeFlag) {
        this.sqlModeFlag = sqlModeFlag;
    }

    public static long convertToFlag(String sqlModeStr) {
        if (sqlModeStr == null) {
            return 0;
        }
        long flag = 0L;

        for (SQLMode sqlMode : SQLMode.values()) {
            if (sqlModeStr.contains(sqlMode.name())) {
                flag |= sqlMode.sqlModeFlag;
            }
        }

        return flag;
    }

    public static Set<SQLMode> convertFromFlag(long sqlModeFlag) {
        Set<SQLMode> sqlModes = new HashSet<>();
        for (SQLMode sqlMode : values()) {
            if ((sqlModeFlag & sqlMode.sqlModeFlag) != 0) {
                sqlModes.add(sqlMode);
            }
        }
        return sqlModes;
    }

    public static boolean isStrictMode(long sqlModeFlags) {
        return SQLModeFlags.check(sqlModeFlags,
            MODE_STRICT_TRANS_TABLES | MODE_STRICT_ALL_TABLES);
    }

    public static boolean isOnlyFullGroupBy(long sqlModeFlags) {
        return SQLModeFlags.check(sqlModeFlags, MODE_ONLY_FULL_GROUP_BY);
    }
}
