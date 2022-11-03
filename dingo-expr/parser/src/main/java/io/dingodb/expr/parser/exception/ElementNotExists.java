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

package io.dingodb.expr.parser.exception;

import io.dingodb.expr.runtime.CompileContext;
import lombok.Getter;

public final class ElementNotExists extends ExprCompileException {
    private static final long serialVersionUID = -3196414862878914396L;
    @Getter
    private final Object index;
    @Getter
    private final CompileContext ctx;

    /**
     * Exception thrown if a given index cannot be found in a given CompileContext.
     *
     * @param index the index
     * @param ctx   the CompileContext
     */
    public ElementNotExists(Object index, CompileContext ctx) {
        super(
            "Element \"" + index + "\" not exist in the following context:\n" + ctx
        );
        this.index = index;
        this.ctx = ctx;
    }
}
