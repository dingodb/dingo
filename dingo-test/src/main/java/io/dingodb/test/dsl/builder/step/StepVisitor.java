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

package io.dingodb.test.dsl.builder.step;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.stream.Collectors;

public interface StepVisitor<T> {
    T visit(@NonNull CustomStep step);

    T visit(@NonNull SqlStringStep step);

    T visit(@NonNull SqlFileStep step);

    T visit(@NonNull SqlFileNameStep step);

    default T visit(@Nullable Step step) {
        return step != null ? step.accept(this) : null;
    }

    default List<T> visitEach(@NonNull List<? extends Step> steps) {
        return steps.stream().map(this::visit).collect(Collectors.toList());
    }
}
