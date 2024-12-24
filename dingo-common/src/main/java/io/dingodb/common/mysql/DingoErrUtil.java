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

package io.dingodb.common.mysql;

import io.dingodb.common.exception.DingoSqlException;
import io.dingodb.common.mysql.error.ErrorMessage;

public final class DingoErrUtil {

    private DingoErrUtil() {
    }

    public static DingoSqlException newStdErr(int errCode, Object... param) {
        DingoSqlException dingoErr = new DingoSqlException(
            errCode, State.mysqlState.getOrDefault(errCode, "HY000"),
            ErrorMessage.errorMap.getOrDefault(errCode, "Unknown error")
        );
        dingoErr.fillErrorByArgs(param);
        return dingoErr;
    }

    public static DingoSqlException newStdErr(String state, int errCode, Object... param) {
        DingoSqlException dingoErr = new DingoSqlException(
            errCode, state, ErrorMessage.errorMap.getOrDefault(errCode, "Unknown error")
        );
        dingoErr.fillErrorByArgs(param);
        return dingoErr;
    }

    public static DingoSqlException newStdErr(String error, int errCode) {
        return new DingoSqlException(
            errCode, State.mysqlState.getOrDefault(errCode, "HY000"), error
        );
    }

    public static DingoSqlException newStdErr(String error) {
        return new DingoSqlException(
            1105, "HY000", error
        );
    }

    public static DingoSqlException newStdErr(int errCode) {
        return new DingoSqlException(
            errCode, State.mysqlState.getOrDefault(errCode, "HY000"),
            ErrorMessage.errorMap.getOrDefault(errCode, "Unknown error")
        );
    }

    public static DingoSqlException newStdErrWithMsg(String msg, int errCode, Object... param) {
        DingoSqlException dingoErr = new DingoSqlException(
            errCode, State.mysqlState.getOrDefault(errCode, "HY000"),
            msg
        );
        dingoErr.fillErrorByArgs(param);
        return dingoErr;
    }


    public static DingoErr newInternalErr(int errCode, Object... param) {
        DingoErr dingoErr = new DingoErr(
            errCode, State.mysqlState.getOrDefault(errCode, "HY000"),
            ErrorMessage.errorMap.getOrDefault(errCode, "Unknown error")
        );
        dingoErr.fillErrorByArgs(param);
        return dingoErr;
    }

    public static DingoErr newInternalErr(String state, int errCode, Object... param) {
        DingoErr dingoErr = new DingoErr(
            errCode, state, ErrorMessage.errorMap.getOrDefault(errCode, "Unknown error")
        );
        dingoErr.fillErrorByArgs(param);
        return dingoErr;
    }

    public static DingoErr newInternalErr(int errCode) {
        return new DingoErr(
            errCode, State.mysqlState.getOrDefault(errCode, "HY000"),
            ErrorMessage.errorMap.getOrDefault(errCode, "Unknown error")
        );
    }

    public static DingoErr newInternalErr(String error) {
        return new DingoErr(
            1105, "HY000", error
        );
    }

    public static DingoErr normal() {
        return new DingoErr(
            0, null, null
        );
    }

    public static DingoSqlException toMysqlError(DingoErr err) {
        return new DingoSqlException(err.errorCode, err.state, err.errorMsg);
    }

    public static DingoErr fromException(Exception exception) {
        int code = 1105;
        String state = "HY000";
        String error;
        if (exception instanceof NullPointerException || exception.getMessage() == null) {
            error = "null point";
        } else if (exception instanceof DingoSqlException) {
            DingoSqlException sqlException = (DingoSqlException) exception;
            return new DingoErr(sqlException.getSqlCode(), sqlException.getSqlState(), sqlException.getMessage());
        } else {
            error = exception.getMessage();
            if (error != null && error.length() > 20) {
                error = error.substring(0, 20);
            }
        }
        return new DingoErr(code, state, error);
    }
}
