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

    private DingoException(@NonNull DingoError err, DingoError reason, String message) {
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
    public static @NonNull DingoException from(DingoError err) {
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
    public static @NonNull DingoException from(DingoError err, String message) {
        return new DingoException(err, message);
    }

    /**
     * Construct DingoException from {@link Throwable}. Result exception will be the given throwable if it is an
     * DingoException, or it will have category {@link DingoError#UNKNOWN} and message, stack trace from that
     * throwable.
     *
     * @param throwable a throwable object
     * @return this throwable if it is DingoException, otherwise a new DingoException with category {@link
     * DingoError#UNKNOWN} and message, stack trace from that throwable.
     */
    public static @NonNull DingoException from(Throwable throwable) {
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
    public static @NonNull DingoException wrap(DingoError err, DingoError reason) {
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
     * exception cause.
     */
    public static @NonNull DingoException wrap(DingoError err, Throwable reason) {
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
     * exception cause.
     */
    public static @NonNull DingoException wrap(DingoError err, DingoException reason) {
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
     * Some virtual machines may, under some circumstances, omit one or more
     * stack frames from the stack trace. In the extreme case, a virtual machine that has no stack trace information
     * concerning this throwable is permitted to return a zero-length array from this method.
     */
    @Override
    public StackTraceElement[] getStackTrace() {
        return super.getStackTrace();
    }
}
