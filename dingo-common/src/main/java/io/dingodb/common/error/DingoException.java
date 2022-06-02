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

import java.util.HashMap;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

/**
 * DingoException is a RuntimeException implementing DingoError. It has stack trace populated by JVM, at the same time
 * it can be treated as a normal DingoError.
 *
 * <p>A common usage of DingoException is to throw it from deep nested function, caught
 * and cast it as DingoError to caller. Following is an example:
 *
 * <pre>{@code
 *     private void doPrivateThing() {
 *         throw new DingoException(SomeErrors.INVALID_REQUEST, "out of range value in field xxx");
 *     }
 *
 *     public DingoError doPublicThing() {
 *         try {
 *             doPrivateThing();
 *         } catch (DingoException err) {
 *             return err;
 *         }
 *         return null;
 *     }
 * }</pre>
 *
 * <p>When logging, cast it back to DingoException if logger support Throwable logging,
 * this way stack trace got printed. Customized logging function is also ok. Such as:
 *
 * <pre>{@code
 *     public void logError(Logger logger, DingoError err) {
 *         if (err is Throwable) {
 *             String info = String.format("error code: %d, info: %s", err.getCode(), err.getInfo())
 *             logger.warn(info, (Throwable)err)
 *         } else {
 *             logger.warn("error code: {}, info: {}, message: {}", err.getCode(), err.getInfo(), err.getMessage());
 *         }
 *     }
 * }</pre>
 */
public class DingoException extends RuntimeException implements IndirectError {
    /**
     * The following exception patterns are used to convert to DingoException from Calcite.
     */

    public static HashMap<Pattern, Integer> EXCEPTION_FROM_CALCITE_CONTEXT_PATTERN_CODE_MAP;
    public static HashMap<Pattern, Integer> RUNTIME_EXCEPTION_PATTERN_CODE_MAP;

    static {
        EXCEPTION_FROM_CALCITE_CONTEXT_PATTERN_CODE_MAP = new HashMap<>();
        // "Table Not Found" from CalciteContextException (90002)
        EXCEPTION_FROM_CALCITE_CONTEXT_PATTERN_CODE_MAP.put(Pattern.compile("Object .* not found"), 90002);
        // "Table Already exists" from CalciteContextException (90007)
        EXCEPTION_FROM_CALCITE_CONTEXT_PATTERN_CODE_MAP.put(Pattern.compile("Table .* already exists"), 90007);
        // "Column Not Found" from  CalciteContextException (90002)
        EXCEPTION_FROM_CALCITE_CONTEXT_PATTERN_CODE_MAP.put(Pattern.compile("Unknown target column.*"), 90002);
        EXCEPTION_FROM_CALCITE_CONTEXT_PATTERN_CODE_MAP.put(Pattern.compile("Column .* not found in any table"), 90002);

        // "Insert Columns more than once" from CalciteContextException (90011)
        EXCEPTION_FROM_CALCITE_CONTEXT_PATTERN_CODE_MAP.put(Pattern.compile("Target column .* is"
            + " assigned more than once"), 90011);
        // Insert Columns not equal" from CalciteContextException  (90013)
        EXCEPTION_FROM_CALCITE_CONTEXT_PATTERN_CODE_MAP.put(Pattern.compile("Number of INSERT target columns "
            + "\\(.*\\) does not equal number"), 90013);

        RUNTIME_EXCEPTION_PATTERN_CODE_MAP = new HashMap<>();
        // "Duplicated Columns" from RuntimeException (90009)
        RUNTIME_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile("Duplicate column names"), 90009);
        // "Create Without Primary Key" from RuntimeException (90010)
        RUNTIME_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile("Primary keys are required"), 90010);
        // "Insert Without Primary Key" from NullPointerException based on RuntimeException (90004)
        RUNTIME_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile("java\\.lang\\.NullPointerException: null"), 90004);
        // "Wrong Function Argument" from RunTimeException(90019)
        RUNTIME_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile(".* does not match"), 90019);
        // "Time Range Error"
        RUNTIME_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile(".* to time/date/datetime"), 90019);
    }
    // TODO
    //public static HashMap<Pattern, Integer> SQL_EXCEPTION_PATTERN_CODE_MAP;
    //public static HashMap<Pattern, Integer> SQL_PARSE_EXCEPTION_PATTERN_CODE_MAP;

    private static final long serialVersionUID = 5564571207617481306L;

    private final DingoError category;
    private final DingoError reason;

    /**
     * Construct DingoException from given error, use given error's basic info as its base info and given message as its
     * message.
     *
     * @param err     error object
     * @param message error message
     */
    private DingoException(DingoError err, String message) {
        this(err, OK, message);
    }

    private DingoException(DingoError err, DingoError reason) {
        this(err, reason, err.getMessage());
    }

    private DingoException(@Nonnull DingoError err, DingoError reason, String message) {
        super(message);
        this.category = err.getCategory();
        this.reason = reason;
    }

    /**
     * Construct DingoException from given error. Result exception will have category and message same as given
     * error's.
     *
     * @param err error object
     */
    @Nonnull
    public static DingoException from(DingoError err) {
        if (err instanceof DingoException) {
            return (DingoException) err;
        }
        return from(err, err.getMessage());
    }

    /**
     * Construct DingoException from given error. Result exception will have category same as given error's and message
     * equal to given message.
     *
     * @param err     error object
     * @param message error message
     */
    @Nonnull
    public static DingoException from(DingoError err, String message) {
        return new DingoException(err, message);
    }

    /**
     * Construct DingoException from {@link Throwable}. Result exception will be the given throwable if it is an
     * DingoException, or it will have category {@link DingoError#UNKNOWN} and message, stack trace from that
     * throwable.
     *
     * @param throwable a throwable object
     * @return this throwable if it is DingoException, otherwise a new DingoException with category {@link
     *     DingoError#UNKNOWN} and message, stack trace from that throwable.
     */
    @Nonnull
    public static DingoException from(Throwable throwable) {
        if (throwable instanceof DingoException) {
            return (DingoException) throwable;
        }
        DingoException ex = new DingoException(UNKNOWN, throwable.getMessage());
        ex.initCause(throwable);
        return ex;
    }

    /**
     * Construct DingoException with category and message from given error and wrap a reason error beneath it.
     *
     * @param err    error object
     * @param reason reason error
     * @return an DingoException with same category and message as given error, but a reason error beneath it
     */
    @Nonnull
    public static DingoException wrap(DingoError err, DingoError reason) {
        if (reason instanceof Throwable) {
            return wrap(err, (Throwable) reason);
        }
        return new DingoException(err, reason);
    }

    /**
     * Construct DingoException with category and message from given error and wrap a throwable as its reason and
     * exception cause.
     *
     * @param err    error object
     * @param reason a throwable reason
     * @return an DingoException with same category and message as given error, with given reason as it's reason and
     *     exception cause.
     */
    @Nonnull
    public static DingoException wrap(DingoError err, Throwable reason) {
        DingoException ex = new DingoException(err, DingoError.from(reason));
        ex.initCause(reason);
        return ex;
    }

    /**
     * Construct DingoException with category and message from given error and wrap an exception as its reason and
     * exception cause.
     *
     * @param err    error object
     * @param reason exception object
     * @return an DingoException with same category and message as given error, with given exception as its reason and
     *     exception cause.
     */
    @Nonnull
    public static DingoException wrap(DingoError err, DingoException reason) {
        return wrap(err, (Throwable) reason);
    }

    @Override
    public DingoError getCategory() {
        return category;
    }

    @Override
    public DingoError getReason() {
        return reason;
    }

    /**
     * Get stack trace from this exception. It is possible for exception to have zero elements in its stack trace.
     *
     * @return stack trace for DingoException, zero length array if no stack traces.
     * @see Throwable#getStackTrace()
     *
     *     <q>Some virtual machines may, under some circumstances, omit one or more
     *     stack frames from the stack trace. In the extreme case, a virtual machine that has no stack trace information
     *     concerning this throwable is permitted to return a zero-length array from this method.</q>
     */
    @Override
    public StackTraceElement[] getStackTrace() {
        return super.getStackTrace();
    }
}
