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

package io.dingodb.driver;

import io.dingodb.common.exception.DingoSqlException;
import io.dingodb.exec.exception.TaskFinException;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.SQLException;

public final class ExceptionUtils {
    // MySQL compatible errors
    private static final int PARSE_ERROR_CODE = 1064;
    private static final String PARSE_ERROR_STATE = "42000";

    // Custom errors
    private static final int NUMBER_FORMAT_ERROR_CODE = 3001;
    private static final int DINGO_EXECUTION_FAIL_ERROR_CODE = 5001;

    private ExceptionUtils() {
    }

    public static @NonNull DingoSqlException toRuntime(@NonNull SQLException exception) {
        return new DingoSqlException(
            exception.getMessage(),
            exception.getErrorCode(),
            exception.getSQLState()
        );
    }

    public static @NonNull DingoSqlException toRuntime(@NonNull SqlParseException exception) {
        Throwable cause = exception.getCause();
        if (cause instanceof CalciteException) {
            return toRuntime((CalciteException) cause);
        }
        return new DingoSqlException(
            exception.getMessage(),
            PARSE_ERROR_CODE,
            PARSE_ERROR_STATE
        );
    }

    /**
     * Construct an {@link DingoSqlException} from a {@link CalciteException} to be shown in client.
     * <p>
     * Calcite message ids are defined in {@link org.apache.calcite.runtime.CalciteResource}.
     * Message are defined in properties files. We prepend string like "Error 1234 (AB123):" to the messages to
     * define SQL_CODE * and SQL_STATE.
     * <p>
     * See {@link org.apache.calcite.runtime.Resources.Resource#create(String, Class)} for details.
     *
     * @param exception the original exception
     * @return the new exception
     */
    public static @NonNull DingoSqlException toRuntime(@NonNull CalciteException exception) {
        return internalToRuntime(exception);
    }

    public static @NonNull DingoSqlException toRuntime(@NonNull TaskFinException exception) {
        return new DingoSqlException(
            exception.getMessage(),
            DINGO_EXECUTION_FAIL_ERROR_CODE,
            DingoSqlException.CUSTOM_ERROR_STATE
        );
    }

    public static @NonNull DingoSqlException toRuntime(@NonNull NumberFormatException exception) {
        return new DingoSqlException(
            exception.getMessage(),
            NUMBER_FORMAT_ERROR_CODE,
            DingoSqlException.CUSTOM_ERROR_STATE
        );
    }

    public static @NonNull DingoSqlException toRuntime(@NonNull RuntimeException exception) {
        // Failed in optimizing, need to know why it is failed.
        if (exception.getMessage().startsWith("Error while applying rule ")) {
            Throwable cause = exception.getCause();
            if (cause != null) {
                return toRuntime(cause);
            }
        }
        return internalToRuntime(exception);
    }

    public static @NonNull DingoSqlException toRuntime(@NonNull Throwable throwable) {
        if (throwable instanceof DingoSqlException) {
            return (DingoSqlException) throwable;
        } else if (throwable instanceof SQLException) {
            return toRuntime((SQLException) throwable);
        } else if (throwable instanceof NumberFormatException) {
            return toRuntime((NumberFormatException) throwable);
        } else if (throwable instanceof TaskFinException) {
            return toRuntime((TaskFinException) throwable);
        } else if (throwable instanceof RuntimeException) {
            return toRuntime((RuntimeException) throwable);
        }
        return internalToRuntime(throwable);
    }

    public static @NonNull SQLException toSql(@NonNull DingoSqlException exception) {
        return new SQLException(
            exception.getMessage(),
            exception.getSqlState(),
            exception.getSqlCode()
        );
    }

    public static @NonNull SQLException toSql(@NonNull Throwable throwable) {
        if (throwable instanceof SQLException) {
            return (SQLException) throwable;
        } else if (throwable instanceof DingoSqlException) {
            return toSql((DingoSqlException) throwable);
        }
        return internalToSql(throwable);
    }

    private static @NonNull DingoSqlException internalToRuntime(@NonNull Throwable throwable) {
        return new DingoSqlException(throwable.getMessage());
    }

    private static @NonNull SQLException internalToSql(@NonNull Throwable throwable) {
        return new SQLException(
            throwable.getMessage(),
            DingoSqlException.CUSTOM_ERROR_STATE,
            DingoSqlException.UNKNOWN_ERROR_CODE
        );
    }

    public static @NonNull RuntimeException wrongSignatureType(
        @NonNull AvaticaStatement statement,
        Meta.@Nullable Signature signature
    ) {
        return new IllegalStateException(
            "Statement \"" + statement.handle + "\" has wrong type of signature: \""
                + (signature != null ? signature.getClass().getName() : "null") + "\"."
        );
    }
}
