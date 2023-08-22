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

package io.dingodb.expr.core;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public final class TypeCode {
    public static final int NULL = 0;
    public static final int INT = 1;
    public static final String INT_NAME = "INT";
    public static final int LONG = 2;
    public static final String LONG_NAME = "LONG";
    public static final int BOOL = 3;
    public static final String BOOL_NAME = "BOOL";
    public static final int FLOAT = 4;
    public static final String FLOAT_NAME = "FLOAT";
    public static final int DOUBLE = 5;
    public static final String DOUBLE_NAME = "DOUBLE";
    public static final int DECIMAL = 6;
    public static final String DECIMAL_NAME = "DECIMAL";
    public static final int STRING = 7;
    public static final String STRING_NAME = "STRING";
    public static final int BINARY = 8;
    public static final int DATE = 101;
    public static final int TIME = 102;
    public static final int TIMESTAMP = 103;
    public static final String ARRAY_NAME = "ARRAY";
    public static final int ARRAY = 1001;
    public static final String LIST_NAME = "LIST";
    public static final int LIST = 1002;
    public static final int MAP = 1003;
    public static final int TUPLE = -1;
    public static final int DICT = -2;
    public static final int OBJECT = 10000;

    private TypeCode() {
    }

    public static @NonNull String nameOf(int code) {
        switch (code) {
            case NULL:
                return "NULL";
            case INT:
                return INT_NAME;
            case LONG:
                return LONG_NAME;
            case BOOL:
                return BOOL_NAME;
            case DOUBLE:
                return DOUBLE_NAME;
            case DECIMAL:
                return DECIMAL_NAME;
            case STRING:
                return STRING_NAME;
            case BINARY:
                return "BINARY";
            case FLOAT:
                return "FLOAT";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                return "TIMESTAMP";
            case ARRAY:
                return ARRAY_NAME;
            case LIST:
                return LIST_NAME;
            case MAP:
                return "MAP";
            case TUPLE:
                return "TUPLE";
            case DICT:
                return "DICT";
            case OBJECT:
                return "OBJECT";
            default:
                break;
        }
        throw new IllegalArgumentException("Unrecognized type code \"" + code + "\".");
    }

    public static int codeOf(@NonNull String name) {
        switch (name) {
            case "NULL":
                return NULL;
            case INT_NAME:
            case "INTEGER":
            case "TINYINT":
                return INT;
            case LONG_NAME:
            case "BIGINT":
                return LONG;
            case BOOL_NAME:
            case "BOOLEAN":
                return BOOL;
            case FLOAT_NAME:
                return FLOAT;
            case DOUBLE_NAME:
            case "REAL":
                return DOUBLE;
            case DECIMAL_NAME:
                return DECIMAL;
            case STRING_NAME:
            case "CHAR":
            case "VARCHAR":
                return STRING;
            case "BINARY":
            case "VARBINARY":
            case "BLOB":
                return BINARY;
            case "DATE":
                return DATE;
            case "TIME":
                return TIME;
            case "TIMESTAMP":
                return TIMESTAMP;
            case "ARRAY":
                return ARRAY;
            case "LIST":
            case "MULTISET":
                return LIST;
            case "MAP":
                return MAP;
            case "TUPLE":
                return TUPLE;
            case "DICT":
                return DICT;
            case "OBJECT":
            case "ANY":
                return OBJECT;
            default:
                break;
        }
        throw new IllegalArgumentException("Unrecognized type name \"" + name + "\".");
    }

    /**
     * Get the type code of a class. {@link List} stands for all its subtypes because they share the same operations,
     * and also {@link Map} for all its subtypes.
     *
     * @param type the Class
     * @return the type code
     */
    public static int codeOf(@Nullable Class<?> type) {
        if (type == null) {
            return NULL;
        }
        if (type.isArray()) {
            // `byte[]` is looked on as a scalar type.
            if (byte[].class.isAssignableFrom(type)) {
                return BINARY;
            } else {
                return ARRAY;
            }
        } else if (int.class.isAssignableFrom(type) || Integer.class.isAssignableFrom(type)) {
            return INT;
        } else if (long.class.isAssignableFrom(type) || Long.class.isAssignableFrom(type)) {
            return LONG;
        } else if (boolean.class.isAssignableFrom(type) || Boolean.class.isAssignableFrom(type)) {
            return BOOL;
        } else if (double.class.isAssignableFrom(type) || Double.class.isAssignableFrom(type)) {
            return DOUBLE;
        } else if (float.class.isAssignableFrom(type) || Float.class.isAssignableFrom(type)) {
            return FLOAT;
        } else if (BigDecimal.class.isAssignableFrom(type)) {
            return DECIMAL;
        } else if (String.class.isAssignableFrom(type)) {
            return STRING;
        } else if (Date.class.isAssignableFrom(type)) {
            return DATE;
        } else if (Time.class.isAssignableFrom(type)) {
            return TIME;
        } else if (Timestamp.class.isAssignableFrom(type)) {
            return TIMESTAMP;
        } else if (List.class.isAssignableFrom(type)) {
            return LIST;
        } else if (Map.class.isAssignableFrom(type)) {
            return MAP;
        }
        return OBJECT;
    }

    /**
     * Get the type code of an Object by get its class first. This method is for dynamically choosing Evaluators in
     * universal evaluator.
     *
     * @param value the Object
     * @return the type code
     */
    public static int getTypeCode(@Nullable Object value) {
        if (value != null) {
            return codeOf(value.getClass());
        }
        return codeOf(Void.class);
    }

    /**
     * Get an array of type codes from an array of values.
     *
     * @param values the array of the values
     * @return the array of the type codes
     */
    public static int @NonNull [] getTypeCodes(Object @NonNull [] values) {
        int[] typeCodes = new int[values.length];
        int i = 0;
        for (Object para : values) {
            typeCodes[i++] = getTypeCode(para);
        }
        return typeCodes;
    }
}
