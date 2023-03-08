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
import java.nio.charset.StandardCharsets;

public final class Casting {
    private Casting() {
    }

    public static int longToInt(long value) {
        return (int) value;
    }

    public static int longToIntRC(long value) {
        int r = longToInt(value);
        if (r == value) {
            return r;
        }
        throw intRangeException(value);
    }

    public static int floatToInt(float value) {
        return Math.round(value);
    }

    public static int floatToIntRC(float value) {
        return longToIntRC(floatToLongRC(value));
    }

    public static int doubleToInt(double value) {
        return (int) Math.round(value);
    }

    public static int doubleToIntRC(double value) {
        return longToIntRC(doubleToLongRC(value));
    }

    public static int boolToInt(boolean value) {
        return value ? 1 : 0;
    }

    public static int decimalToInt(@NonNull BigDecimal value) {
        return value.setScale(0, RoundingMode.HALF_UP).intValue();
    }

    public static int decimalToIntRC(@NonNull BigDecimal value) {
        return longToIntRC(decimalToLongRC(value));
    }

    public static int stringToInt(@NonNull String value) {
        return Integer.parseInt(value);
    }

    public static long intToLong(int value) {
        return value;
    }

    public static long floatToLong(float value) {
        return Math.round(value);
    }

    public static long floatToLongRC(float value) {
        long r = floatToLong(value);
        if (r != Long.MIN_VALUE && r != Long.MAX_VALUE) {
            return r;
        }
        throw longRangeException(value);
    }

    public static long doubleToLong(double value) {
        return Math.round(value);
    }

    public static long doubleToLongRC(double value) {
        long r = doubleToLong(value);
        if (r != Long.MIN_VALUE && r != Long.MAX_VALUE) {
            return r;
        }
        throw longRangeException(value);
    }

    public static long boolToLong(boolean value) {
        return value ? 1L : 0L;
    }

    public static long decimalToLong(@NonNull BigDecimal value) {
        return value.setScale(0, RoundingMode.HALF_UP).longValue();
    }

    public static long decimalToLongRC(@NonNull BigDecimal value) {
        long r = decimalToLong(value);
        if (BigDecimal.valueOf(Long.MIN_VALUE).compareTo(value) <= 0
            && value.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) <= 0) {
            return r;
        }
        throw longRangeException(value);
    }

    public static long stringToLong(@NonNull String value) {
        return Long.parseLong(value);
    }

    public static boolean intToBool(int value) {
        return value != 0;
    }

    public static boolean longToBool(long value) {
        return value != 0L;
    }

    public static boolean doubleToBool(double value) {
        return value != 0.0;
    }

    public static boolean floatToBool(float value) {
        return value != 0.0f;
    }

    public static boolean decimalToBool(@NonNull BigDecimal value) {
        return value.compareTo(BigDecimal.ZERO) != 0;
    }

    public static float intToFloat(int value) {
        return value;
    }

    public static float longToFloat(long value) {
        return (float) value;
    }

    public static float boolToFloat(boolean value) {
        return value ? 1.0f : 0.0f;
    }

    public static float decimalToFloat(@NonNull BigDecimal value) {
        return value.floatValue();
    }

    public static float stringToFloat(@NonNull String value) {
        return Float.parseFloat(value);
    }

    public static double intToDouble(int value) {
        return value;
    }

    public static double longToDouble(long value) {
        return (double) value;
    }

    public static double floatToDouble(float value) {
        return Double.parseDouble(String.valueOf(value));
    }
    public static double boolToDouble(boolean value) {
        return value ? 1.0 : 0.0;
    }

    public static double decimalToDouble(@NonNull BigDecimal value) {
        return value.doubleValue();
    }

    public static double stringToDouble(@NonNull String value) {
        return Double.parseDouble(value);
    }

    public static @NonNull BigDecimal intToDecimal(int value) {
        return BigDecimal.valueOf(value);
    }

    public static @NonNull BigDecimal longToDecimal(long value) {
        return BigDecimal.valueOf(value);
    }

    public static @NonNull BigDecimal floatToDecimal(float value) {
        return BigDecimal.valueOf(value);
    }

    public static @NonNull BigDecimal doubleToDecimal(double value) {
        return BigDecimal.valueOf(value);
    }

    public static BigDecimal boolToDecimal(boolean value) {
        return value ? BigDecimal.ONE : BigDecimal.ZERO;
    }

    public static @NonNull BigDecimal stringToDecimal(@NonNull String value) {
        return new BigDecimal(value);
    }

    public static @NonNull String intToString(int value) {
        return Integer.toString(value);
    }

    public static @NonNull String longToString(long value) {
        return Long.toString(value);
    }

    public static @NonNull String floatToString(float value) {
        return Float.toString(value);
    }

    public static @NonNull String doubleToString(double value) {
        return Double.toString(value);
    }

    public static @NonNull String boolToString(boolean value) {
        return Boolean.toString(value);
    }

    public static @NonNull String decimalToString(@NonNull BigDecimal value) {
        return value.toPlainString();
    }

    public static @NonNull String binaryToString(byte @NonNull [] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    public static byte @NonNull [] stringToBinary(@NonNull String value) {
        return value.getBytes(StandardCharsets.UTF_8);
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
