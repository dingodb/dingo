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

import io.dingodb.expr.runtime.RtExpr;
import lombok.Getter;

public final class InvalidIndex extends ExprCompileException {
    private static final long serialVersionUID = 152335684688384361L;

    @Getter
    private final RtExpr rtExpr;

    /**
     * Exception thrown when the index for IndexOp is not valid.
     *
     * @param index the index
     */
    public InvalidIndex(RtExpr index) {
        super("Index value must be a const here, but is: \"" + index + "\".");
        this.rtExpr = index;
    }
}
