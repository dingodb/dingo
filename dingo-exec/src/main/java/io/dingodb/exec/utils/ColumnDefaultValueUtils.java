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

package io.dingodb.exec.utils;

import io.dingodb.common.time.DingoTimeZoneContext;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.ListType;
import io.dingodb.common.type.MapType;
import io.dingodb.common.type.scalar.BinaryType;
import io.dingodb.common.type.scalar.BitType;
import io.dingodb.common.type.scalar.BooleanType;
import io.dingodb.common.type.scalar.DateType;
import io.dingodb.common.type.scalar.DecimalType;
import io.dingodb.common.type.scalar.DoubleType;
import io.dingodb.common.type.scalar.FloatType;
import io.dingodb.common.type.scalar.IntegerType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.type.scalar.StringType;
import io.dingodb.common.type.scalar.TimeType;
import io.dingodb.common.type.scalar.TimestampType;
import io.dingodb.expr.common.timezone.core.DateTimeType;
import io.dingodb.expr.common.timezone.processor.DingoTimeZoneProcessor;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;

public class ColumnDefaultValueUtils {

    private ColumnDefaultValueUtils() {

    }
    public static Object getDefaultValue(DingoType type) {
        DingoTimeZoneProcessor processor = DingoTimeZoneContext.getProcessor();
        if (type instanceof StringType) {
            return "";
        } else if (type instanceof LongType) {
            return 0L;
        } else if (type instanceof IntegerType) {
            return 0;
        } else if (type instanceof DoubleType) {
            return 0D;
        } else if (type instanceof FloatType) {
            return 0F;
        } else if (type instanceof DecimalType) {
            return new BigDecimal(0);
        } else if (type instanceof DateType) {
            return processor.processDateTime("0000-00-00", DateTimeType.DATE);
        } else if (type instanceof BooleanType) {
            return false;
        } else if (type instanceof TimestampType) {
            return processor.processDateTime("0000-00-00 00:00:00", DateTimeType.TIMESTAMP);
        } else if (type instanceof TimeType) {
            return processor.processDateTime("00:00:00", DateTimeType.TIME);
        } else if (type instanceof ListType) {
            return new ArrayList<>();
        } else if (type instanceof MapType) {
            return new LinkedHashMap<>();
        } else if (type instanceof BitType) {
            return 0L;
        } else if (type instanceof BinaryType) {
            return "00000000".getBytes();
        }
        return null;
    }
}
