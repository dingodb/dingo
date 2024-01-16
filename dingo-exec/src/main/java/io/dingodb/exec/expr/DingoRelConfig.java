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

package io.dingodb.exec.expr;

import io.dingodb.exec.fun.DingoFunFactory;
import io.dingodb.expr.parser.ExprParser;
import io.dingodb.expr.rel.RelConfig;
import io.dingodb.expr.runtime.ExprCompiler;
import lombok.Getter;

public class DingoRelConfig implements RelConfig {
    private static final ExprParser EXPR_PARSER = new ExprParser(DingoFunFactory.getInstance());

    @Getter
    private final DingoEvalContext evalContext;

    public DingoRelConfig() {
        evalContext = new DingoEvalContext();
    }

    @Override
    public ExprParser getExprParser() {
        return EXPR_PARSER;
    }

    @Override
    public ExprCompiler getExprCompiler() {
        return RelConfig.super.getExprCompiler();
    }
}
