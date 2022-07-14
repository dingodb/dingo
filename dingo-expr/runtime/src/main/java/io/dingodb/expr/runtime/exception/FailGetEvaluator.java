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

package io.dingodb.expr.runtime.exception;

import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.expr.runtime.evaluator.base.EvaluatorFactory;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

@RequiredArgsConstructor
@Getter
public final class FailGetEvaluator extends Exception {
    private static final long serialVersionUID = -3103060977223789004L;

    private final EvaluatorFactory factory;
    private final int[] paraTypeCodes;

    @Nonnull
    @Override
    public String getMessage() {
        return "Cannot find evaluator in \""
            + factory.getClass().getSimpleName()
            + "\" for parameter types ("
            + Arrays.stream(paraTypeCodes).mapToObj(TypeCode::nameOf).collect(Collectors.joining(", "))
            + ").";
    }
}
