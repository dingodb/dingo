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

package io.dingodb.expr.parser;

import io.dingodb.expr.parser.op.Op;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.op.RtOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.function.Function;

public interface FunFactory {
    /**
     * Get the function (Op) by its name.
     *
     * @param funName the name of the function
     * @return the function (Op)
     */
    @NonNull Op getFun(String funName);

    /**
     * Register a user defined function.
     *
     * @param funName     the name of the function
     * @param funSupplier a function to create the runtime function object
     */
    void registerUdf(
        @NonNull String funName,
        final @NonNull Function<RtExpr[], RtOp> funSupplier
    );
}
