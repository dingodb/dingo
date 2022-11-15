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

import java.math.BigDecimal;
import java.math.RoundingMode;

public final class Casting {
    private Casting() {
    }

    public static int longToInt(long value) {
        if ((int) value == value) {
            return (int) value;
        }
        throw intRangeException(value);
    }

    public static int doubleToInt(double value) {
        return longToInt(doubleToLong(value));
    }

    public static int decimalToInt(@NonNull BigDecimal value) {
        BigDecimal n = value.setScale(0, RoundingMode.HALF_UP);
        int result = n.intValue();
        if (n.compareTo(BigDecimal.valueOf(result)) == 0) {
            return result;
        }
        throw intRangeException(value);
    }

    public static long intToLong(int value) {
        return value;
    }

    public static long doubleToLong(double value) {
        long result = Math.round(value);
        if (result != Long.MIN_VALUE && result != Long.MAX_VALUE) {
            return result;
        }
        throw longRangeException(value);
    }

    public static long decimalToLong(@NonNull BigDecimal value) {
        BigDecimal n = value.setScale(0, RoundingMode.HALF_UP);
        long result = n.longValue();
        if (n.compareTo(BigDecimal.valueOf(result)) == 0) {
            return result;
        }
        throw longRangeException(value);
    }

    public static double intToDouble(int value) {
        return value;
    }

    public static double longToDouble(long value) {
        return (double) value;
    }

    public static double decimalToDouble(@NonNull BigDecimal value) {
        return value.doubleValue();
    }

    public static @NonNull BigDecimal intToDecimal(int value) {
        return BigDecimal.valueOf(value);
    }

    public static @NonNull BigDecimal longToDecimal(long value) {
        return BigDecimal.valueOf(value);
    }

    public static @NonNull BigDecimal doubleToDecimal(double value) {
        return BigDecimal.valueOf(value);
    }

    public static @NonNull ArithmeticException intRangeException(Object value) {
        return new ArithmeticException(
            "Value " + value + " exceeds limits of integers, which is from "
                + Integer.MIN_VALUE + " to " + Integer.MAX_VALUE + "."
        );
    }

    public static @NonNull ArithmeticException longRangeException(Object value) {
        return new ArithmeticException(
            "Value " + value + " exceeds limits of longs, which is from "
                + Long.MIN_VALUE + " to " + Long.MAX_VALUE + "."
        );
    }
}
