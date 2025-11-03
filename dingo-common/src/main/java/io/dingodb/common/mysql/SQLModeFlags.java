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

public class SQLModeFlags {

    public static final long MODE_REAL_AS_FLOAT = 1;
    public static final long MODE_PIPES_AS_CONCAT = 2;
    public static final long MODE_ANSI_QUOTES = 4;
    public static final long MODE_IGNORE_SPACE = 8;
    public static final long MODE_NOT_USED = 16;
    public static final long MODE_ONLY_FULL_GROUP_BY = 32;
    public static final long MODE_NO_UNSIGNED_SUBTRACTION = 64;
    public static final long MODE_NO_DIR_IN_CREATE = 128;
    public static final long MODE_POSTGRESQL = 256;
    public static final long MODE_ORACLE = 512;
    public static final long MODE_MSSQL = 1024;
    public static final long MODE_DB2 = 2048;
    public static final long MODE_MAXDB = 4096;
    public static final long MODE_NO_KEY_OPTIONS = 8192;
    public static final long MODE_NO_TABLE_OPTIONS = 16384;
    public static final long MODE_NO_FIELD_OPTIONS = 32768;
    public static final long MODE_MYSQL323 = 65536;
    public static final long MODE_MYSQL40 = (MODE_MYSQL323 * 2);
    public static final long MODE_ANSI = (MODE_MYSQL40 * 2);
    public static final long MODE_NO_AUTO_VALUE_ON_ZERO = (MODE_ANSI * 2);
    public static final long MODE_NO_BACKSLASH_ESCAPES = (MODE_NO_AUTO_VALUE_ON_ZERO * 2);
    public static final long MODE_STRICT_TRANS_TABLES = (MODE_NO_BACKSLASH_ESCAPES * 2);
    public static final long MODE_STRICT_ALL_TABLES = (MODE_STRICT_TRANS_TABLES * 2);

    public static final long MODE_NO_ZERO_IN_DATE = (MODE_STRICT_ALL_TABLES * 2);
    public static final long MODE_NO_ZERO_DATE = (MODE_NO_ZERO_IN_DATE * 2);
    public static final long MODE_INVALID_DATES = (MODE_NO_ZERO_DATE * 2);
    public static final long MODE_ERROR_FOR_DIVISION_BY_ZERO = (MODE_INVALID_DATES * 2);
    public static final long MODE_TRADITIONAL = (MODE_ERROR_FOR_DIVISION_BY_ZERO * 2);
    public static final long MODE_NO_AUTO_CREATE_USER = (MODE_TRADITIONAL * 2);
    public static final long MODE_HIGH_NOT_PRECEDENCE = (MODE_NO_AUTO_CREATE_USER * 2);
    public static final long MODE_NO_ENGINE_SUBSTITUTION = (MODE_HIGH_NOT_PRECEDENCE * 2);
    public static final long MODE_PAD_CHAR_TO_FULL_LENGTH = (1L << 31);

    public static boolean check(long flags, long toCheck) {
        return (flags & toCheck) != 0;
    }
}
