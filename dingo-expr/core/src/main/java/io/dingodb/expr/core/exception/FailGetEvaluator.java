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

package io.dingodb.expr.core.exception;

import io.dingodb.expr.core.evaluator.EvaluatorFactory;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@Getter
public final class FailGetEvaluator extends IllegalParaTypes {
    private static final long serialVersionUID = -3103060977223789004L;

    private final EvaluatorFactory factory;

    public FailGetEvaluator(@NonNull EvaluatorFactory factory, int @Nullable [] paraTypeCodes) {
        super("Cannot find evaluator in \"" + factory.getClass().getSimpleName() + "\" of ", paraTypeCodes);
        this.factory = factory;
    }
}
