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

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

final class ErrorUtil {
    private ErrorUtil() {
    }

    static String getDetailMessage(@NonNull DingoError err) {
        DingoError reason = err.getReason();
        if (reason.isOk()) {
            return err.getMessage();
        }
        StringBuilder builder = new StringBuilder(err.getMessage());
        do {
            builder.append(". Caused by\n");
            builder.append(reason.getMessage());
            reason = reason.getReason();
        }
        while (!reason.isOk());
        return builder.toString();
    }


    static @Nullable String format(@NonNull FormattingError err, Object @NonNull ... args) {
        String fmt = err.getFormat();
        if (args.length == 0 && fmt == null) {
            return null;
        }
        if (fmt == null) {
            return String.format("No formation for error type %s", err.getClass().getCanonicalName());
        }
        return String.format(fmt, args);
    }

    static String toString(@NonNull DingoError err) {
        String className = err.getClazz().getCanonicalName();
        if (err.isCategory()) {
            return String.format("error class: %s, code: %s, info: %s",
                className, err.getCode(), err.getInfo());
        }
        return String.format("error class: %s, code: %d, info: %s, message: %s",
            className, err.getCode(), err.getInfo(), err.getMessage());
    }
}
