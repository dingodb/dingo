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

package io.dingodb.common.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;

public class MySqlUtils {
    public final static Charset GBK                 = Charset.forName("GBK");
    public final static Charset BIG5                 = Charset.forName("BIG5");
    public final static Charset UTF8                 = Charset.forName("UTF-8");
    public final static Charset UTF16                = Charset.forName("UTF-16");
    public final static Charset UTF32                = Charset.forName("UTF-32");
    public final static Charset ASCII                = Charset.forName("ASCII");
    public final static Charset GB18030              = Charset.forName("GB18030");
    public final static Charset LATIN1               = Charset.forName("ISO-8859-1");

    public static BigDecimal decimal(BigDecimal value, int precision, int scale) {
        int v_scale = value.scale();

        int v_precision;
        if (v_scale > scale) {
            value = value.setScale(scale, BigDecimal.ROUND_HALF_UP);
            v_precision = value.precision();
        } else {
            v_precision = value.precision();
        }

        int v_ints = v_precision - v_scale;
        int ints = precision - scale;

        if (v_precision > precision || v_ints > ints) {
            boolean sign = value.signum() > 0;

            if (scale == 1) {
                return sign ? MAX_DEC_1[ints] : MIN_DEC_1[ints];
            }

            if (scale == 2) {
                return sign ? MAX_DEC_2[ints] : MIN_DEC_2[ints];
            }

            return new BigDecimal(
                sign ? MAX_INT[precision - 1] : MIN_INT[precision - 1]
                , scale
            );
        }

        return value;
    }

    private static BigInteger[] MAX_INT = {
        new BigInteger("9"),
        new BigInteger("99"),
        new BigInteger("999"),
        new BigInteger("9999"),
        new BigInteger("99999"),
        new BigInteger("999999"),
        new BigInteger("9999999"),
        new BigInteger("99999999"),
        new BigInteger("999999999"),
        new BigInteger("9999999999"),
        new BigInteger("99999999999"),
        new BigInteger("999999999999"),
        new BigInteger("9999999999999"),
        new BigInteger("99999999999999"),
        new BigInteger("999999999999999"),
        new BigInteger("9999999999999999"),
        new BigInteger("99999999999999999"),
        new BigInteger("999999999999999999"),
        new BigInteger("9999999999999999999"),
        new BigInteger("99999999999999999999"),
        new BigInteger("999999999999999999999"),
        new BigInteger("9999999999999999999999"),
        new BigInteger("99999999999999999999999"),
        new BigInteger("999999999999999999999999"),
        new BigInteger("9999999999999999999999999"),
        new BigInteger("99999999999999999999999999"),
        new BigInteger("999999999999999999999999999"),
        new BigInteger("9999999999999999999999999999"),
        new BigInteger("99999999999999999999999999999"),
        new BigInteger("999999999999999999999999999999"),
        new BigInteger("9999999999999999999999999999999"),
        new BigInteger("99999999999999999999999999999999"),
        new BigInteger("999999999999999999999999999999999"),
        new BigInteger("9999999999999999999999999999999999"),
        new BigInteger("99999999999999999999999999999999999"),
        new BigInteger("999999999999999999999999999999999999"),
        new BigInteger("9999999999999999999999999999999999999"),
        new BigInteger("99999999999999999999999999999999999999"),
    };

    private static BigInteger[] MIN_INT = {
        new BigInteger("-9"),
        new BigInteger("-99"),
        new BigInteger("-999"),
        new BigInteger("-9999"),
        new BigInteger("-99999"),
        new BigInteger("-999999"),
        new BigInteger("-9999999"),
        new BigInteger("-99999999"),
        new BigInteger("-999999999"),
        new BigInteger("-9999999999"),
        new BigInteger("-99999999999"),
        new BigInteger("-999999999999"),
        new BigInteger("-9999999999999"),
        new BigInteger("-99999999999999"),
        new BigInteger("-999999999999999"),
        new BigInteger("-9999999999999999"),
        new BigInteger("-99999999999999999"),
        new BigInteger("-999999999999999999"),
        new BigInteger("-9999999999999999999"),
        new BigInteger("-99999999999999999999"),
        new BigInteger("-999999999999999999999"),
        new BigInteger("-9999999999999999999999"),
        new BigInteger("-99999999999999999999999"),
        new BigInteger("-999999999999999999999999"),
        new BigInteger("-9999999999999999999999999"),
        new BigInteger("-99999999999999999999999999"),
        new BigInteger("-999999999999999999999999999"),
        new BigInteger("-9999999999999999999999999999"),
        new BigInteger("-99999999999999999999999999999"),
        new BigInteger("-999999999999999999999999999999"),
        new BigInteger("-9999999999999999999999999999999"),
        new BigInteger("-99999999999999999999999999999999"),
        new BigInteger("-999999999999999999999999999999999"),
        new BigInteger("-9999999999999999999999999999999999"),
        new BigInteger("-99999999999999999999999999999999999"),
        new BigInteger("-999999999999999999999999999999999999"),
        new BigInteger("-9999999999999999999999999999999999999"),
        new BigInteger("-99999999999999999999999999999999999999"),
    };

    private static BigDecimal[] MAX_DEC_1 = {
        new BigDecimal("0.9"),
        new BigDecimal("9.9"),
        new BigDecimal("99.9"),
        new BigDecimal("999.9"),
        new BigDecimal("9999.9"),
        new BigDecimal("99999.9"),
        new BigDecimal("999999.9"),
        new BigDecimal("9999999.9"),
        new BigDecimal("99999999.9"),
        new BigDecimal("999999999.9"),
        new BigDecimal("9999999999.9"),
        new BigDecimal("99999999999.9"),
        new BigDecimal("999999999999.9"),
        new BigDecimal("9999999999999.9"),
        new BigDecimal("99999999999999.9"),
        new BigDecimal("999999999999999.9"),
        new BigDecimal("9999999999999999.9"),
        new BigDecimal("99999999999999999.9"),
        new BigDecimal("999999999999999999.9"),
        new BigDecimal("9999999999999999999.9"),
        new BigDecimal("99999999999999999999.9"),
        new BigDecimal("999999999999999999999.9"),
        new BigDecimal("9999999999999999999999.9"),
        new BigDecimal("99999999999999999999999.9"),
        new BigDecimal("999999999999999999999999.9"),
        new BigDecimal("9999999999999999999999999.9"),
        new BigDecimal("99999999999999999999999999.9"),
        new BigDecimal("999999999999999999999999999.9"),
        new BigDecimal("9999999999999999999999999999.9"),
        new BigDecimal("99999999999999999999999999999.9"),
        new BigDecimal("999999999999999999999999999999.9"),
        new BigDecimal("9999999999999999999999999999999.9"),
        new BigDecimal("99999999999999999999999999999999.9"),
        new BigDecimal("999999999999999999999999999999999.9"),
        new BigDecimal("9999999999999999999999999999999999.9"),
        new BigDecimal("99999999999999999999999999999999999.9"),
        new BigDecimal("999999999999999999999999999999999999.9"),
        new BigDecimal("9999999999999999999999999999999999999.9"),
    };

    private static BigDecimal[] MIN_DEC_1 = {
        new BigDecimal("-0.9"),
        new BigDecimal("-9.9"),
        new BigDecimal("-99.9"),
        new BigDecimal("-999.9"),
        new BigDecimal("-9999.9"),
        new BigDecimal("-99999.9"),
        new BigDecimal("-999999.9"),
        new BigDecimal("-9999999.9"),
        new BigDecimal("-99999999.9"),
        new BigDecimal("-999999999.9"),
        new BigDecimal("-9999999999.9"),
        new BigDecimal("-99999999999.9"),
        new BigDecimal("-999999999999.9"),
        new BigDecimal("-9999999999999.9"),
        new BigDecimal("-99999999999999.9"),
        new BigDecimal("-999999999999999.9"),
        new BigDecimal("-9999999999999999.9"),
        new BigDecimal("-99999999999999999.9"),
        new BigDecimal("-999999999999999999.9"),
        new BigDecimal("-9999999999999999999.9"),
        new BigDecimal("-99999999999999999999.9"),
        new BigDecimal("-999999999999999999999.9"),
        new BigDecimal("-9999999999999999999999.9"),
        new BigDecimal("-99999999999999999999999.9"),
        new BigDecimal("-999999999999999999999999.9"),
        new BigDecimal("-9999999999999999999999999.9"),
        new BigDecimal("-99999999999999999999999999.9"),
        new BigDecimal("-999999999999999999999999999.9"),
        new BigDecimal("-9999999999999999999999999999.9"),
        new BigDecimal("-99999999999999999999999999999.9"),
        new BigDecimal("-999999999999999999999999999999.9"),
        new BigDecimal("-9999999999999999999999999999999.9"),
        new BigDecimal("-99999999999999999999999999999999.9"),
        new BigDecimal("-999999999999999999999999999999999.9"),
        new BigDecimal("-9999999999999999999999999999999999.9"),
        new BigDecimal("-99999999999999999999999999999999999.9"),
        new BigDecimal("-999999999999999999999999999999999999.9"),
        new BigDecimal("-9999999999999999999999999999999999999.9"),
    };

    private static BigDecimal[] MAX_DEC_2 = {
        new BigDecimal("0.99"),
        new BigDecimal("9.99"),
        new BigDecimal("99.99"),
        new BigDecimal("999.99"),
        new BigDecimal("9999.99"),
        new BigDecimal("99999.99"),
        new BigDecimal("999999.99"),
        new BigDecimal("9999999.99"),
        new BigDecimal("99999999.99"),
        new BigDecimal("999999999.99"),
        new BigDecimal("9999999999.99"),
        new BigDecimal("99999999999.99"),
        new BigDecimal("999999999999.99"),
        new BigDecimal("9999999999999.99"),
        new BigDecimal("99999999999999.99"),
        new BigDecimal("999999999999999.99"),
        new BigDecimal("9999999999999999.99"),
        new BigDecimal("99999999999999999.99"),
        new BigDecimal("999999999999999999.99"),
        new BigDecimal("9999999999999999999.99"),
        new BigDecimal("99999999999999999999.99"),
        new BigDecimal("999999999999999999999.99"),
        new BigDecimal("9999999999999999999999.99"),
        new BigDecimal("99999999999999999999999.99"),
        new BigDecimal("999999999999999999999999.99"),
        new BigDecimal("9999999999999999999999999.99"),
        new BigDecimal("99999999999999999999999999.99"),
        new BigDecimal("999999999999999999999999999.99"),
        new BigDecimal("9999999999999999999999999999.99"),
        new BigDecimal("99999999999999999999999999999.99"),
        new BigDecimal("999999999999999999999999999999.99"),
        new BigDecimal("9999999999999999999999999999999.99"),
        new BigDecimal("99999999999999999999999999999999.99"),
        new BigDecimal("999999999999999999999999999999999.99"),
        new BigDecimal("9999999999999999999999999999999999.99"),
        new BigDecimal("99999999999999999999999999999999999.99"),
        new BigDecimal("999999999999999999999999999999999999.99"),
        new BigDecimal("9999999999999999999999999999999999999.99"),
    };

    private static BigDecimal[] MIN_DEC_2 = {
        new BigDecimal("-0.99"),
        new BigDecimal("-9.99"),
        new BigDecimal("-99.99"),
        new BigDecimal("-999.99"),
        new BigDecimal("-9999.99"),
        new BigDecimal("-99999.99"),
        new BigDecimal("-999999.99"),
        new BigDecimal("-9999999.99"),
        new BigDecimal("-99999999.99"),
        new BigDecimal("-999999999.99"),
        new BigDecimal("-9999999999.99"),
        new BigDecimal("-99999999999.99"),
        new BigDecimal("-999999999999.99"),
        new BigDecimal("-9999999999999.99"),
        new BigDecimal("-99999999999999.99"),
        new BigDecimal("-999999999999999.99"),
        new BigDecimal("-9999999999999999.99"),
        new BigDecimal("-99999999999999999.99"),
        new BigDecimal("-999999999999999999.99"),
        new BigDecimal("-9999999999999999999.99"),
        new BigDecimal("-99999999999999999999.99"),
        new BigDecimal("-999999999999999999999.99"),
        new BigDecimal("-9999999999999999999999.99"),
        new BigDecimal("-99999999999999999999999.99"),
        new BigDecimal("-999999999999999999999999.99"),
        new BigDecimal("-9999999999999999999999999.99"),
        new BigDecimal("-99999999999999999999999999.99"),
        new BigDecimal("-999999999999999999999999999.99"),
        new BigDecimal("-9999999999999999999999999999.99"),
        new BigDecimal("-99999999999999999999999999999.99"),
        new BigDecimal("-999999999999999999999999999999.99"),
        new BigDecimal("-9999999999999999999999999999999.99"),
        new BigDecimal("-99999999999999999999999999999999.99"),
        new BigDecimal("-999999999999999999999999999999999.99"),
        new BigDecimal("-9999999999999999999999999999999999.99"),
        new BigDecimal("-99999999999999999999999999999999999.99"),
        new BigDecimal("-999999999999999999999999999999999999.99"),
        new BigDecimal("-9999999999999999999999999999999999999.99"),
    };
}
