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

package io.dingodb.common.error;

import java.util.Objects;

final class WrappedError extends AbstractError implements IndirectError {
    private final DingoError err;
    private final DingoError reason;

    WrappedError(DingoError err, DingoError reason) {
        this.err = Objects.requireNonNull(err, "null error to wrap the cause");
        this.reason = Objects.requireNonNull(reason, "null cause wrap underneath the error");
    }

    @Override
    public DingoError getCategory() {
        return err.getCategory();
    }

    @Override
    public String getMessage() {
        return err.getMessage();
    }

    @Override
    public DingoError getReason() {
        return reason;
    }
}
