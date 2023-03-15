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
import org.checkerframework.checker.nullness.qual.Nullable;

public class SessionVariableFun extends RtStringFun {
    protected SessionVariableFun(RtExpr[] paras) {
        super(paras);
    }

    @Override
    protected @Nullable Object fun(@NonNull Object @NonNull [] values) {
        return null;
    }
}
