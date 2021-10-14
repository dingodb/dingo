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

package io.dingodb.expr.parser.op;

import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.op.RtOp;

import java.util.function.Function;

final class RtOpWrapper extends Op {
    private final Function<RtExpr[], RtOp> rtOpMaker;

    RtOpWrapper(OpType type, Function<RtExpr[], RtOp> rtOpMaker) {
        super(type);
        this.rtOpMaker = rtOpMaker;
    }

    RtOpWrapper(String name, Function<RtExpr[], RtOp> rtOpMaker) {
        super(name);
        this.rtOpMaker = rtOpMaker;
    }

    @Override
    protected RtOp createRtOp(RtExpr[] rtExprArray) {
        return rtOpMaker.apply(rtExprArray);
    }
}
