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

import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.VariadicOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serial;
import javax.annotation.Nullable;

public class ConcatWsFun extends VariadicOp {

    public static final ConcatWsFun INSTANCE = new ConcatWsFun();
    public static final String NAME = "CONCAT_WS";
    @Serial
    private static final long serialVersionUID = -2732756541981148391L;

    private ConcatWsFun() {
    }

    @Nullable
    public static String eval(Object @NonNull [] args) {
        if (args.length < 2) {
            return null;
        }
        // First argument is separator.
        Object sepObj = args[0];
        if (sepObj == null) {
            // MySQL: if separator is NULL, result is NULL.
            return null;
        }
        String separator = sepObj.toString();

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (int i = 1; i < args.length; i++) {
            Object val = args[i];
            if (val == null) {
                // Skip NULL values (non-separator).
                continue;
            }
            if (!first) {
                sb.append(separator);
            }
            sb.append(val);
            first = false;
        }
        return sb.toString();
    }

    @Override
    public Object evalValue(Object @NonNull [] values, ExprConfig config) {
        if (values.length < 2) {
            return null;
        }

        Object sepObj = values[0];
        if (sepObj == null) {
            return null;
        }
        String separator = sepObj.toString();

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (int i = 1; i < values.length; i++) {
            Object val = values[i];
            if (val == null) {
                // Skip NULL values (non-separator).
                continue;
            }
            if (!first) {
                sb.append(separator);
            }
            sb.append(val);
            first = false;
        }
        return sb.toString();
    }

    @Override
    public OpKey keyOf(@NonNull Type @NonNull ... types) {
        return Types.ANY;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
