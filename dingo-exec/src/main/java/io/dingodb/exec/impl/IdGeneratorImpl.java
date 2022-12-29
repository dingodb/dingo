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

package io.dingodb.exec.impl;

import io.dingodb.exec.base.Id;
import io.dingodb.exec.base.IdGenerator;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class IdGeneratorImpl implements IdGenerator {
    private long currentValue = 0;

    public @NonNull Id get(String idPrefix) {
        return new Id(idPrefix + "-" + String.format("%04X", currentValue++));
    }

    @Override
    public @NonNull Id get() {
        return new Id(String.format("%04X", currentValue++));
    }
}
