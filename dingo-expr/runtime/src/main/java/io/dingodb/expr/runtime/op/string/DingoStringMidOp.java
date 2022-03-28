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

package io.dingodb.expr.runtime.op.string;

import io.dingodb.expr.runtime.RtExpr;

import javax.annotation.Nonnull;

public class DingoStringMidOp extends RtStringConversionOp {
    private static final long serialVersionUID = 8185618697441684894L;

    /**
     * Create an DingoStringMidOp. get the mid of string.
     *
     * @param paras the parameters of the op
     */
    public DingoStringMidOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Nonnull
    @Override
    protected Object fun(@Nonnull Object[] values) {
        String inputStr = ((String)values[0]).trim();
        int startIndex = ((Long)(values[1])).intValue();
        int cnt = ((Long)(values[2])).intValue();

        /**
         * 1. start Index is invalid, return ""
         */
        if (startIndex <= 0 || startIndex > inputStr.length()) {
            return "";
        }

        /**
         * 2. input String is null || "" || startIndex > inputStr.length()
         */
        if (inputStr == null || inputStr.equals("") || startIndex > inputStr.length()) {
            return "";
        } else {
            int endIndex = (startIndex + cnt - 1 > inputStr.length() ? inputStr.length() : startIndex + cnt - 1);
            return inputStr.substring(startIndex - 1, endIndex);
        }
    }
}
