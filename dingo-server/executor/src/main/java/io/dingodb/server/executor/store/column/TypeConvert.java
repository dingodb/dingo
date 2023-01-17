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

package io.dingodb.server.executor.store.column;

import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.scalar.BinaryType;
import io.dingodb.common.type.scalar.BooleanType;
import io.dingodb.common.type.scalar.DecimalType;
import io.dingodb.common.type.scalar.DoubleType;
import io.dingodb.common.type.scalar.IntegerType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.type.scalar.StringType;
import io.dingodb.common.type.scalar.TimestampType;
import io.dingodb.strorage.column.ColumnConstant;

import java.math.BigDecimal;

public class TypeConvert {
    public static int DingoTypeToCKType(DingoType dt) {
        if (dt instanceof BooleanType) {
            return ColumnConstant.ColumnTypeBool;
        } else if (dt instanceof DecimalType) {
            return ColumnConstant.ColumnTypeFloat32;
        } else if (dt instanceof DoubleType) {
            return ColumnConstant.ColumnTypeFloat64;
        } else if (dt instanceof IntegerType) {
            return ColumnConstant.ColumnTypeInt32;
        } else if (dt instanceof LongType) {
            return ColumnConstant.ColumnTypeInt64;
        } else if (dt instanceof StringType) {
            return ColumnConstant.ColumnTypeString;
        } else if (dt instanceof TimestampType) {
            return ColumnConstant.ColumnTypeInt64;
        }

        throw new RuntimeException("Unsupported type: " + dt.getClass());
    }

    public static Object StringToDingoData(DingoType dt, final String str) {
        if (str == null) {
            throw new RuntimeException("CK return null str");
        }

        if (dt instanceof BooleanType) {
            return Boolean.valueOf(str);
        } else if (dt instanceof DecimalType) {
            return new BigDecimal(str);
        } else if (dt instanceof DoubleType) {
            return Double.valueOf(str);
        } else if (dt instanceof IntegerType) {
            return Integer.valueOf(str);
        } else if (dt instanceof LongType) {
            return Long.valueOf(str);
        } else if (dt instanceof StringType) {
            return str;
        } else if (dt instanceof TimestampType) {
            return Long.valueOf(str);
        }
        throw new RuntimeException("Unsupported type: " + dt.getClass() + ", str: " + str);
    }

    public static String DingoDataToString(DingoType dt, Object obj) {
        if (obj == null) {
            throw new RuntimeException("Obj is null while dingo data to string.");
        }

        if (dt instanceof BinaryType) {
            return obj.toString();
        } else if (dt instanceof BooleanType) {
            return obj.toString();
        } else if (dt instanceof DecimalType) {
            return obj.toString();
        } else if (dt instanceof DoubleType) {
            return obj.toString();
        } else if (dt instanceof IntegerType) {
            return obj.toString();
        } else if (dt instanceof LongType) {
            return obj.toString();
        } else if (dt instanceof StringType) {
            return obj.toString();
        } else if (dt instanceof TimestampType) {
            return obj.toString();
        }

        throw new RuntimeException("Unsupported type" + obj.getClass());
    }
}
