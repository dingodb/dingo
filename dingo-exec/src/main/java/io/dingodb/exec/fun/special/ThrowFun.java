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

package io.dingodb.exec.fun.special;

import io.dingodb.common.exception.DingoSqlException;
import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.NullaryOp;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ThrowFun extends NullaryOp {
    public static final ThrowFun INSTANCE = new ThrowFun();
    public static final String NAME = "THROW";

    private static final long serialVersionUID = -2796011351737125199L;

    @Override
    public Object eval(EvalContext context, ExprConfig config) {
        throw new DingoSqlException(
            "Thrown intentionally for testing.",
            DingoSqlException.TEST_ERROR_CODE,
            DingoSqlException.CUSTOM_ERROR_STATE
        );
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
