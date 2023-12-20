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

import java.util.HashMap;
import java.util.Map;

public class ColumnType {

    public static int FIELD_TYPE_DECIMAL = 0;
    public static int FIELD_TYPE_TINY = 1;
    public static int FIELD_TYPE_SHORT = 2;
    public static int FIELD_TYPE_LONG = 3;
    public static int FIELD_TYPE_FLOAT = 4;
    public static int FIELD_TYPE_DOUBLE = 5;
    public static int FIELD_TYPE_NULL = 6;
    public static int FIELD_TYPE_TIMESTAMP = 7;
    public static int FIELD_TYPE_LONGLONG = 8;
    public static int FIELD_TYPE_INT24 = 9;
    public static int FIELD_TYPE_DATE = 10;
    public static int FIELD_TYPE_TIME = 11;
    public static int FIELD_TYPE_DATETIME = 12;
    public static int FIELD_TYPE_YEAR = 13;
    public static int FIELD_TYPE_NEWDATE = 14;
    public static int FIELD_TYPE_VARCHAR = 15;
    public static int FIELD_TYPE_BIT = 16;
    public static int FIELD_TYPE_NEW_DECIMAL = 246;
    public static int FIELD_TYPE_ENUM = 247;
    public static int FIELD_TYPE_SET = 248;
    public static int FIELD_TYPE_TINY_BLOB = 249;
    public static int FIELD_TYPE_MEDIUM_BLOB = 250;
    public static int FIELD_TYPE_LONG_BLOB = 251;
    public static int FIELD_TYPE_BLOB = 252;
    public static int FIELD_TYPE_VAR_STRING = 253;
    public static int FIELD_TYPE_STRING = 254;
    public static int FIELD_TYPE_GEOMETRY = 255;

    public static final Map<String, Integer> typeMapping = new HashMap<>();

    static {
        typeMapping.put("INTEGER", FIELD_TYPE_LONG);
        typeMapping.put("BIGINT", FIELD_TYPE_LONGLONG);
        typeMapping.put("FLOAT", FIELD_TYPE_FLOAT);
        typeMapping.put("VARCHAR", FIELD_TYPE_VAR_STRING);
        typeMapping.put("DOUBLE", FIELD_TYPE_DOUBLE);
        typeMapping.put("DATE", FIELD_TYPE_DATE);
        typeMapping.put("TIME", FIELD_TYPE_TIME);
        typeMapping.put("TIMESTAMP", FIELD_TYPE_TIMESTAMP);
        typeMapping.put("CHAR", FIELD_TYPE_STRING);
        typeMapping.put("ARRAY", FIELD_TYPE_VAR_STRING);
        typeMapping.put("MULTISET", FIELD_TYPE_SET);
        typeMapping.put("BOOLEAN", FIELD_TYPE_TINY);
        typeMapping.put("VARBINARY", FIELD_TYPE_BLOB);
    }

}
