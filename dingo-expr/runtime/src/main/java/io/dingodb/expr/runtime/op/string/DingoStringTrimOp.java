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

public class DingoStringTrimOp extends RtStringConversionOp {
    private static final long serialVersionUID = -4332118553191666565L;

    /**
     * Create an DingoStringTrimOp.
     * DingoStringTrimOp trim the leading and trailing blanks of a String by {@code String::trim}.
     *
     * @param paras the parameters of the op
     */
    public DingoStringTrimOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Nonnull
    @Override
    protected Object fun(@Nonnull Object[] values) {
        if (values.length == 3) {
            String opType = (String)values[0];
            String deleteStr = (String)values[1];
            String inputStr = (String)values[2];

            int startIndex = 0;
            int endIndex = inputStr.length() - 1;
            switch (opType) {
                case "BOTH": {
                    for (; startIndex < inputStr.length(); startIndex++) {
                        if (inputStr.charAt(startIndex) != deleteStr.charAt(0)) {
                            break;
                        }
                    }
                    for (; endIndex > 0; endIndex--) {
                        if (inputStr.charAt(endIndex) != deleteStr.charAt(0)) {
                            break;
                        }
                    }
                    break;
                }
                case "LEADING": {
                    for (; startIndex < inputStr.length(); startIndex++) {
                        if (inputStr.charAt(startIndex) != deleteStr.charAt(0)) {
                            break;
                        }
                    }
                    break;
                }
                case "TRAILING": {
                    for (; endIndex > 0; endIndex--) {
                        if (inputStr.charAt(endIndex) != deleteStr.charAt(0)) {
                            break;
                        }
                    }
                    break;
                }
                default:
                    break;
            }
            return inputStr.substring(startIndex, endIndex + 1);
        } else {
            return ((String) values[0]).trim();
        }
    }
}
