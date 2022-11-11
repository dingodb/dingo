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
import org.checkerframework.checker.nullness.qual.NonNull;

public class RTrimFun extends RtStringFun {
    public static final String NAME = "rtrim";
    private static final long serialVersionUID = -7445709112049015539L;

    /**
     * Create an DingoStringTrimOp.
     * DingoStringTrimOp trim the trailing blanks of a String.
     *
     * @param paras the parameters of the op
     */
    public RTrimFun(RtExpr[] paras) {
        super(paras);
    }

    public static String trimRight(final String str) {
        if (str == null || str.equals("")) {
            return str;
        } else {
            int startIndex = 0;
            int endIndex = str.length() - 1;
            for (; endIndex >= 0; endIndex--) {
                if (str.charAt(endIndex) != ' ') {
                    break;
                }
            }
            return str.substring(startIndex, endIndex + 1);
        }
    }

    @Override
    protected Object fun(Object @NonNull [] values) {
        String str = (String) values[0];
        return trimRight(str);
    }
}
