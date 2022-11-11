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
public class ConcatFun extends RtStringFun {
    public static final String NAME = "concat";
    private static final long serialVersionUID = 5454356467741754567L;

    /**
     * Create an DingoStringConcatOp. concat all input args.
     *
     * @param paras the parameters of the op
     */
    public ConcatFun(RtExpr[] paras) {
        super(paras);
    }

    public static @NonNull String concat(String str1, String str2) {
        return str1 + str2;
    }

    @Override
    protected Object fun(Object @NonNull [] values) {
        String inputStr1 = (values[0] == null ? "" : values[0].toString());
        String inputStr2 = (values[1] == null ? "" : values[1].toString());
        return concat(inputStr1, inputStr2);
    }
}
