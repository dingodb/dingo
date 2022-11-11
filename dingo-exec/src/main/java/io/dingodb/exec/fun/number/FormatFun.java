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

package io.dingodb.exec.fun.number;

import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.op.RtStringFun;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Slf4j
public class FormatFun extends RtStringFun {
    public static final String NAME = "format";
    private static final long serialVersionUID = 4805636716328583550L;

    public FormatFun(RtExpr[] paras) {
        super(paras);
    }

    public static String formatNumber(final double value, int scale) {
        if (scale < 0) {
            scale = 0;
        }

        BigDecimal decimal = new BigDecimal(value).setScale(scale, RoundingMode.HALF_UP);
        return decimal.toString();
    }

    @Override
    protected Object fun(Object @NonNull [] values) {
        int inputScalar = new BigDecimal(String.valueOf(values[1]))
            .setScale(0, RoundingMode.HALF_UP).intValue();
        if (inputScalar < 0) {
            inputScalar = 0;
        }

        BigDecimal decimal = new BigDecimal(String.valueOf(values[0])).setScale(inputScalar, RoundingMode.HALF_UP);
        return formatNumber(decimal.doubleValue(), inputScalar);
    }
}
