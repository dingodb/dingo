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

import java.util.function.Supplier;

/**
 * A public auxiliary interface derived from DingoError to provide formation support for customized message.
 *
 * <p>Note: Currently, it uses {@link String#format(String, Object...)},
 * but for error message formation it is overkilling and inconvenient. A simple placeholder substitution with position
 * and named parameter placeholder support would be better.
 */
public interface FormattingError extends DingoError {

    /**
     * Throw an exception with customized message built from formation of {@link FormattingError#getFormat()} with given
     * arguments, if {@link DingoError#getCode()} equals {@link NoError#getCode()} then skip throw.
     *
     * @param args arguments to format string
     */
    default void throwFormatError(Object... args) {
        if (getCode() == NoError.OK.getCode()) {
            return;
        }
        throw this.formatAsException(args);
    }

    /**
     * Get format string for customized message formation.
     *
     * @return format string.
     */
    String getFormat();

    /**
     * Construct an error with customized message built from formation of {@link #getFormat()} with given arguments.
     *
     * @param args arguments to format string
     * @return a customized DingoError with message formatted from {@link #getFormat()} and given parameters.
     */
    default DingoError format(Object... args) {
        String message = ErrorUtil.format(this, args);
        if (message == null) {
            return this;
        }
        return DingoError.from(this, message);
    }

    /**
     * Construct an exception with customized message built from formation of {@link #getFormat()} with given
     * arguments.
     *
     * @param args arguments to format string
     * @return a customized {@link DingoException} with message formatted from {@link #getFormat()} and given
     *     parameters.
     */
    default DingoException formatAsException(Object... args) {
        String message = ErrorUtil.format(this, args);
        if (message == null) {
            return DingoException.from(this);
        }
        return DingoException.from(this, message);
    }

    /**
     * Construct an exception supplier with customized message built from formation of {@link #getFormat()} with given
     * arguments.
     *
     * @param args arguments to format string
     * @return a customized {@link Supplier} as {@link DingoException} with message formatted from {@link #getFormat()}
     *      and given parameters.
     */
    default Supplier<RuntimeException> supplierException(FormattingError error, Object... args) {
        return () -> error.formatAsException(args);
    }

}
