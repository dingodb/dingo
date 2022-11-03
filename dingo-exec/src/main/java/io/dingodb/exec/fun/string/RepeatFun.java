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
import java.util.Collections;

@Slf4j
public class RepeatFun extends RtStringFun {
    public static final String NAME = "repeat";
    private static final long serialVersionUID = 7673054922107009329L;

    /**
     * Create an DingoStringRepeatOp. repeat the String times.
     *
     * @param paras the parameters of the op
     */
    public RepeatFun(RtExpr[] paras) {
        super(paras);
    }

    public static String repeatString(final String inputStr, int times) {
        if (times < 0) {
            return "";
        }

        if (inputStr == null || inputStr.equals("")) {
            return inputStr;
        } else {
            return String.join("", Collections.nCopies(times, inputStr));
        }
    }

    @Override
    protected Object fun(Object @NonNull [] values) {
        String inputStr = ((String) values[0]);
        int times = new BigDecimal(String.valueOf(values[1])).setScale(0, RoundingMode.HALF_UP).intValue();

        return repeatString(inputStr, times);
    }
}
