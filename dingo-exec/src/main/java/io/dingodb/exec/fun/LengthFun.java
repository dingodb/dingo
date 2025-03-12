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

package io.dingodb.exec.fun;

import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.UnaryOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serial;

public class LengthFun extends UnaryOp {
    public static final LengthFun INSTANCE = new LengthFun();

    public static final String NAME = "length";
    @Serial
    private static final long serialVersionUID = 1518305554642713272L;

    @Override
    public Object evalValue(Object value, ExprConfig config) {
        if (value == null) {
            return null;
        } else {
            return value.toString().getBytes().length;
        }
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
