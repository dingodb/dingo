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
public class SubstringFun extends RtStringFun {
    public static final String NAME = "substring";
    private static final long serialVersionUID = 9195218471853466015L;

    /**
     * Create an DingoSubStringOp. RtTrimOp trim the leading and trailing blanks of a String by {@code String::trim}.
     *
     * @param paras the parameters of the op
     */
    public SubstringFun(RtExpr[] paras) {
        super(paras);
    }

    public static @NonNull String subStr(final String inputStr, final int index, final int cnt) {
        if (inputStr == null || inputStr.isEmpty()) {
            return "";
        }

        if (cnt < 0) {
            return "";
        }

        int startIndex = index;
        if (startIndex < 0) {
            startIndex = startIndex + inputStr.length() + 1;
        }
        startIndex = startIndex - 1;

        if (startIndex + cnt > inputStr.length()) {
            if (startIndex == inputStr.length()) {
                startIndex = startIndex + 1;
            }
            return inputStr.substring(startIndex);
        }

        return inputStr.substring(startIndex, startIndex + cnt);
    }

    @Override
    protected Object fun(Object @NonNull [] values) {
        String inputStr = ((String) values[0]);

        BigDecimal decimal = new BigDecimal(values[1].toString());
        int startIndex = decimal.setScale(0, RoundingMode.HALF_UP).intValue();

        decimal = new BigDecimal(values[2].toString());
        int cnt = decimal.setScale(0, RoundingMode.HALF_UP).intValue();

        return subStr(inputStr, startIndex, cnt);
    }
}
