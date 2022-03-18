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

public class DingoSubStringOp extends RtStringConversionOp {
    private static final long serialVersionUID = 9195218471853466015L;

    /**
     * Create an DingoSubStringOp. RtTrimOp trim the leading and trailing blanks of a String by {@code String::trim}.
     *
     * @param paras the parameters of the op
     */
    public DingoSubStringOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Nonnull
    @Override
    protected Object fun(@Nonnull Object[] values) {
        String inputStr = ((String) values[0]).trim();
        Integer startIndex = Integer.valueOf(values[1].toString());
        Integer cnt = Integer.valueOf(values[2].toString());
        if (startIndex + cnt > inputStr.length()) {
            System.out.println("Substring OutOfRange. Input:" + inputStr + ", startIndex:"
                + startIndex + ", subCnt:" + cnt);
        }
        return inputStr.substring(startIndex, startIndex + cnt);
    }
}
