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

public class DingoStringLTrimOp extends RtStringConversionOp {
    private static final long serialVersionUID = -8557732786466948967L;

    /**
     * Create an DingoStringLTrimOp.
     * DingoStringTrimOp trim the leading blanks of a String.
     *
     * @param paras the parameters of the op
     */
    public DingoStringLTrimOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Nonnull
    @Override
    protected Object fun(@Nonnull Object[] values) {
        int startIndex = 0;
        String inputStr = (String)values[0];
        int endIndex = inputStr.length() - 1;

        for (; startIndex < inputStr.length(); startIndex++) {
            if (inputStr.charAt(startIndex) != ' ') {
                break;
            }
        }

        return inputStr.substring(startIndex, endIndex + 1);
    }
}
