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
import io.dingodb.expr.runtime.op.RtStringFun;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

@Slf4j
public class RtLowerFun extends RtStringFun {
    private static final long serialVersionUID = -3546267408443749483L;

    /**
     * Create an RtToLowerCaseOp. RtToLowerCaseOp converts a String to lower case by {@code String::toLowerCase}.
     *
     * @param paras the parameters of the op
     */
    public RtLowerFun(RtExpr[] paras) {
        super(paras);
    }

    @Override
    protected Object fun(@NonNull Object @NonNull [] values) {
        return ((String) values[0]).toLowerCase();
    }
}
