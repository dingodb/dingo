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

@Slf4j
public class ReplaceFun extends RtStringFun {
    public static final String NAME = "replace";
    private static final long serialVersionUID = 5736080205736344227L;

    /**
     * Create an RtReplaceOp. DingoStringReplaceOp performs string replacing operation by {@code String::replace}.
     *
     * @param paras the parameters of the op
     */
    public ReplaceFun(RtExpr[] paras) {
        super(paras);
    }

    public static String replaceStr(final String inputStr, final String str1, final String str2) {
        if (inputStr == null || inputStr.equals("")) {
            return inputStr;
        }

        return inputStr.replace(str1, str2);
    }

    @Override
    protected Object fun(Object @NonNull [] values) {
        return replaceStr((String) values[0], (String) values[1], (String) values[2]);
    }
}
