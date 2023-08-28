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

package io.dingodb.test.dsl.builder.checker;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.PolyNull;

public interface SqlCheckerVisitor<T> {
    T visit(@NonNull SqlObjectResultChecker sqlChecker);

    T visit(@NonNull SqlResultCountChecker sqlChecker);

    T visit(@NonNull SqlCsvStringResultChecker sqlChecker);

    T visit(@NonNull SqlCsvFileResultChecker sqlChecker);

    T visit(@NonNull SqlCsvFileNameResultChecker sqlChecker);

    T visit(@NonNull SqlUpdateCountChecker sqlChecker);

    T visit(@NonNull SqlExceptionChecker sqlChecker);

    T visit(@NonNull SqlResultDumper sqlChecker);

    default @PolyNull T safeVisit(@PolyNull SqlChecker sqlChecker) {
        return sqlChecker != null ? sqlChecker.accept(this) : null;
    }
}
