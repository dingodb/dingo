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

package io.dingodb.exec.fun.string;

import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.op.RtStringFun;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Slf4j
public class MidFun extends RtStringFun {
    public static final String NAME = "mid";
    private static final long serialVersionUID = 8185618697441684894L;

    /**
     * Create an DingoStringMidOp. get the mid of string.
     *
     * @param paras the parameters of the op
     */
    public MidFun(RtExpr[] paras) {
        super(paras);
    }

    public static @NonNull String midString(final String inputStr, int startIndex) {
        if (inputStr == null || inputStr.length() == 0) {
            return "";
        }

        if (startIndex < 0) {
            startIndex = startIndex + inputStr.length() + 1;
        }

        if (startIndex - 1 == inputStr.length()) {
            startIndex = startIndex + 2;
        }

        return inputStr.substring(startIndex - 1);
    }

    public static @NonNull String midString(final String inputStr, int startIndex, int cnt) {
        if (inputStr == null || inputStr.length() == 0 || cnt < 0) {
            return "";
        }

        if (startIndex < 0) {
            startIndex = startIndex + inputStr.length() + 1;
        }

        int endIndex = Math.min(startIndex + cnt - 1, inputStr.length());
        if (startIndex - 1 == inputStr.length()) {
            startIndex = startIndex + 2;
        }

        return inputStr.substring(startIndex - 1, endIndex);
    }

    @Override
    protected Object fun(Object @NonNull [] values) {
        String inputStr = ((String) values[0]);

        BigDecimal decimal = new BigDecimal(values[1].toString());
        int startIndex = decimal.setScale(0, RoundingMode.HALF_UP).intValue();

        if (values.length == 2) {
            return midString(inputStr, startIndex);
        }

        decimal = new BigDecimal(values[2].toString());
        int cnt = decimal.setScale(0, RoundingMode.HALF_UP).intValue();
        return midString(inputStr, startIndex, cnt);
    }
}
