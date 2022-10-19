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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ExceptionInfo {
    private final String name;
    private final String message;
    private final List<String> stacktrace;

    private ExceptionInfo(String name, String message, List<String> stacktrace) {
        this.name = name;
        this.message = message;
        this.stacktrace = stacktrace;
    }

    /**
     * Build an exception info from given throwable.
     *
     * @param throwable a throwable object
     * @return an exception info
     */
    public static @NonNull ExceptionInfo fromThrowable(Throwable throwable) {
        String name = buildExceptionName(throwable);
        List<String> stacktrace = buildExceptionStacktrace(throwable);
        return new ExceptionInfo(name, throwable.getMessage(), stacktrace);
    }

    /**
     * Build an exception info chain from given throwable and its cause chain.
     *
     * @param throwable an throwable object
     * @return an exception info chain
     */
    public static @NonNull List<ExceptionInfo> buildExceptionChain(Throwable throwable) {
        List<ExceptionInfo> chain = new ArrayList<>();
        do {
            chain.add(fromThrowable(throwable));
            throwable = throwable.getCause();
        }
        while (throwable != null);
        return Collections.unmodifiableList(chain);
    }

    private static String buildExceptionName(Throwable throwable) {
        if (throwable instanceof DingoError) {
            DingoError err = (DingoError) throwable;
            return String.format("%s[%s]", throwable.getClass().getName(), err.getClazz().getName());
        }
        return throwable.getClass().getCanonicalName();
    }

    private static @NonNull List<String> buildExceptionStacktrace(@NonNull Throwable throwable) {
        List<String> stacktrace = new ArrayList<>();
        for (StackTraceElement element : throwable.getStackTrace()) {
            String stack = String.format("%s.%s at %s:%d",
                element.getClassName(), element.getMethodName(),
                element.getFileName(), element.getLineNumber());
            stacktrace.add(stack);
        }
        return stacktrace;
    }

    public String getName() {
        return name;
    }

    public String getMessage() {
        return message;
    }

    public List<String> getStacktrace() {
        return stacktrace;
    }
}
