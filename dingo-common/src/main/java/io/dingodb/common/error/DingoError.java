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

/**
 * DingoError is designed as a universal interface, so we use it without fighting with a flat one-dimension enum error
 * code system, while that system can be easily built upon this interface, we don't encourage it. We encourage that
 * every module should have their own flat enum errors in API boundary, and resolve errors from other modules to errors
 * belong to their API before propagation. This way each module has their own errors bound to their API, without
 * interfering with modules in encompassing system.
 *
 * <p>A typical flat errors can be defined as following:</p>
 *
 * <pre>
 * { @code
 *     public enum ValidationError implements FormattingError {
 *         CONFLICT_NAME(1, "Conflict vertex name", "Conflict vertex name %s between vertex type %s and %s"),
 *         CYCLIC_GRAPH(2, "Cyclic graph", "Cyclic graph in pipeline: %s");
 *
 *         private int code;
 *         private String info;
 *         private String format;
 *
 *         &#64;Override
 *         public int getCode() {
 *             return code;
 *         }
 *
 *         &#64;Override
 *         public String getInfo() {
 *             return info;
 *         }
 *
 *         &#64;Override
 *         public String getFormat() {
 *             return format;
 *         }
 *
 *         ValidationError(int code, String info) {
 *             this(code, info, null);
 *         }
 *
 *         ValidationError(int code, String info, String message) {
 *             this.code = code;
 *             this.info = info;
 *             this.message = message;
 *         }
 *     }
 * }</pre>
 *
 * <p>In building flat error system, DingoError splits errors to category errors
 * and context errors. Category errors are numerable and context free, while context errors hold context in addition to
 * its category. For example, in above code, `ValidationError.CONFLICT_NAME` is a category error, while
 * `DingoError.from(ValidationError.CONFLICT_NAME, "conflicting name xyz")` is a context error. Category of an error is
 * retrieved by getCategory() method, category of a category error is itself. For errors belong to same category, their
 * categories should be reference equal, e.g. ==, to each other. This can be easily achieved by implementing category
 * errors as enum values.
 *
 * <p>To test whether a given error is some kind of error, use {@link #is(Class)}
 * and {@link #is(DingoError)}.
 *
 * <p>Errors with same category have same {@link #getCode()}, {@link #getInfo()}
 * but possible different {@link #getMessage()}.</p>
 *
 * <p>DingoError reserves error code 0 and -1 for internal usage. Error system
 * implementers MUST NOT use 0 and -1 as error codes, otherwise methods {@link #isOk()} and {@link #isUnknown()} may not
 * function correctly.
 *
 * <p>DingoError suggests that function using DingoError as return type
 * never return a null DingoError. In case of no error, return {@link #OK}. There is no null DingoError. For this sake,
 * there is no null safe method provided by this package.
 *
 * <p>In module boundary, all errors from one module should be resolved to errors
 * in caller module for propagation except {@link #OK}, otherwise modules interacting with caller module will be
 * confused with those unknown errors.
 *
 * <p>The least required methods are {@link #getCode()} and {@link #getInfo()},
 * which provide error code and basic error info for category errors. Context errors inherit the info from associated
 * category errors.
 */
public interface DingoError {

    /**
     * OK means that there is no error. This is the only error that can be propagated cross module boundary.
     */
    DingoError OK = NoError.OK;

    /**
     * UNKNOWN provides error category for all unknown errors, it is a bridge between DingoError and other error
     * systems, such as {@link Throwable}. Clients can construct an unknown error with customized message and wrap it
     * beneath exported error. This way error handling module knows what happens underneath without exporting
     * implementation detail to clients.
     *
     * <pre>{@code
     * try {
     *     doSomething();
     * } catch (DingoException ex) {
     *     return AppError.SOME_ERROR.format(ex.getMessage);
     * } catch (Throwable throwable) {
     *     DingoError cause = DingoError.from(throwable);
     *     return DingoError.wrap(AppError.INTERNAL_ERROR, cause);
     * }
     * }</pre>
     */
    DingoError UNKNOWN = UnknownError.UNKNOWN;

    /**
     * Construct an error with category of given error and message from given message. Thus result error will have
     * category same as given error's and message equal to given message.
     *
     * @param err     an error object
     * @param message customized error message
     * @return an error object with category same as given error's and message equal to given message.
     */
    static @NonNull DingoError from(DingoError err, String message) {
        return new DerivedError(err, message);
    }

    /**
     * Construct an error from {@link Throwable}. If this throwable object is {@link DingoError}, return it as is,
     * otherwise construct an error from {@link #UNKNOWN} with message of this throwable.
     *
     * @param throwable a throwable object
     * @return this throwable object if it is an {@link DingoError}, otherwise construct an error with message from this
     *     throwable, and {@link #UNKNOWN} as its category.
     */
    static @NonNull DingoError from(Throwable throwable) {
        if (throwable instanceof DingoError) {
            return (DingoError) throwable;
        }
        return DingoError.from(DingoError.UNKNOWN, throwable.getMessage());
    }

    /**
     * Hide reason error beneath given error. The reason error will be treated as implementation detail for result
     * error. Its error message only show in result error's {@link #getDetailMessage()} but not {@link #getMessage()}.
     * If cause error contains only information related to caller's request context, {@link #from(DingoError, String)}
     * should be used instead of this function, {@link FormattingError#format(Object...)} is also an option. Reason
     * error can be retrieved from {@link #getReason()} later.
     *
     * @param err    error object to which nest cause.
     * @param reason error object to be nested to given error
     * @return an error object behaves same as given error except {@link #getDetailMessage()}.
     */
    static @NonNull DingoError wrap(DingoError err, DingoError reason) {
        return new WrappedError(err, reason);
    }

    /**
     * Convenient function to provide default string formation. For exuberant diagnostic message, use custom logging
     * function, since this function **does not contain stack trace** in result string for DingoException.
     *
     * @param err error object
     * @return informational string includes class name, error code, error info and possible error message.
     */
    static String toString(DingoError err) {
        return ErrorUtil.toString(err);
    }

    /**
     * Get error code from this error.
     *
     * @return integer representation defined by underlying error category.
     */
    int getCode();

    /**
     * Get basic info of this error.
     *
     * @return basic info defined by underlying error category.
     */
    String getInfo();

    /**
     * Whether this error object represents a successful operation result.
     *
     * @return true if error code equals to 0, otherwise false.
     */
    default boolean isOk() {
        return this == OK;
    }

    /**
     * Whether this error is an {@link #UNKNOWN} error, eg whether its {@link #getCategory()} is {@link #UNKNOWN}.
     *
     * @return true if this error is an {@link #UNKNOWN} error, false otherwise.
     */
    default boolean isUnknown() {
        return getCategory() == UNKNOWN;
    }

    /**
     * Whether this error is same as given error or derived it.
     *
     * @param err error or error category
     * @return true if this error is same as given error or derived it
     */
    default boolean is(DingoError err) {
        return this == err || getCategory() == err;
    }

    /**
     * Whether this error is derived from given error category class.
     *
     * @param clazz class of error category
     * @param <T>   type of error category
     * @return true if this error derived from given error category class.
     */
    default <T extends Enum<T> & DingoError> boolean is(@NonNull Class<T> clazz) {
        return clazz.isAssignableFrom(getClazz());
    }

    /**
     * Cast this error as given error category class if it is derived from that error class. The result error may not
     * same as this error and thus may have different message comparing to this error.
     *
     * @param clazz class of error category
     * @param <T>   type of error category
     * @return error object if this error derived from given error category class, otherwise null
     */
    @SuppressWarnings("unchecked")
    default <T extends Enum<T> & DingoError> T as(@NonNull Class<T> clazz) {
        DingoError category = getCategory();
        if (clazz.isAssignableFrom(category.getClass())) {
            return (T) category;
        }
        return null;
    }

    /**
     * Get category of this error.
     *
     * @return underlying error category.
     */
    default DingoError getCategory() {
        return this;
    }

    /**
     * Whether this error is a category error.
     *
     * @return true if this error is a category error, false otherwise.
     */
    default boolean isCategory() {
        return this == getCategory();
    }

    /**
     * Shorthand for {@code getCategory().getClass() }.
     *
     * @return class of category error
     */
    default Class<? extends DingoError> getClazz() {
        return this.getCategory().getClass();
    }

    /**
     * Get error message from this error, default to {@link #getInfo()} if no customized message.
     *
     * @return error message if specified, otherwise default to getInfo().
     */
    default String getMessage() {
        return getInfo();
    }

    /**
     * Get error messages from this error and its cause errors. Default to {@link #getMessage()} if this error has no
     * cause.
     *
     * @return detail message
     */
    default String getDetailMessage() {
        return ErrorUtil.getDetailMessage(this);
    }

    /**
     * Get reason error beneath this error if there is one, {@link #OK} otherwise.
     *
     * @return reason error beneath this error or {@link #OK}.
     */
    default DingoError getReason() {
        return DingoError.OK;
    }

    /**
     * Degrade this error to an error with no reason error, no stack trace. The resulting error will share category and
     * message with this error.
     *
     * @return an error object with no reason error, no stack trace, but same message as this error.
     */
    default DingoError degrade() {
        // Use `==` intentionally here to handle two cases:
        // * a category error or
        // * DingoException constructed with no customized message
        // We don't handle message equality case, this is rare, and thus
        // avoid comparing every time.
        if (getMessage() == getCategory().getMessage()) {
            return getCategory();
        }
        return from(getCategory(), getMessage());
    }

    /**
     * Throw an exception with {@link DingoError},
     * if {@link DingoError#getCode()} equals {@link NoError#getCode()} then skip throw.
     */
    default void throwError() {
        if (getCode() == NoError.OK.getCode()) {
            return;
        }
        throw DingoException.from(this);
    }

    /**
     * Construct an exception with customized message.
     *
     * @return a customized {@link DingoException} with message
     */
    default DingoException asException() {
        return asException(null);
    }

    /**
     * Construct an exception with customized message.
     *
     * @param message error message
     * @return a customized {@link DingoException} with message
     */
    default DingoException asException(String message) {
        if (message == null) {
            return DingoException.from(this);
        }
        return DingoException.from(this, message);
    }
}
