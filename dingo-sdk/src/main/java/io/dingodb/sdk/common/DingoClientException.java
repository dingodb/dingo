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

package io.dingodb.sdk.common;

import io.dingodb.sdk.utils.ResultCode;

public class DingoClientException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    protected int resultCode = ResultCode.CLIENT_ERROR;

    public DingoClientException(int resultCode, String message) {
        super(message);
        this.resultCode = resultCode;
    }

    public DingoClientException(int resultCode, Throwable exception) {
        super(exception);
        this.resultCode = resultCode;
    }

    public DingoClientException(int resultCode) {
        super();
        this.resultCode = resultCode;
    }

    public DingoClientException(int resultCode, String message, Throwable ex) {
        super(message, ex);
        this.resultCode = resultCode;
    }

    public DingoClientException(String message, Throwable ex) {
        super(message, ex);
    }

    public DingoClientException(String message) {
        super(message);
    }

    public DingoClientException(Throwable ex) {
        super(ex);
    }

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder(512);
        final String message = super.getMessage();

        sb.append("Error ");
        sb.append(resultCode);
        sb.append(": ");
        if (message != null) {
            sb.append(message);
        }
        return sb.toString();
    }

    /**
     * Get integer result code.
     */
    public final int getResultCode() {
        return resultCode;
    }


    /**
     * Exception thrown when client can't parse data returned from server.
     */
    public static final class Parse extends DingoClientException {
        private static final long serialVersionUID = 1L;

        public Parse(String message) {
            super(ResultCode.PARSE_ERROR, message);
        }
    }

    /**
     * Exception thrown when namespace is invalid.
     */
    public static final class InvalidTableName extends DingoClientException {
        private static final long serialVersionUID = 1L;

        public InvalidTableName(String tableName) {
            super(ResultCode.INVALID_TABLE_NAME,
                tableName == null || tableName.length() == 0 ? "Invalid tableName is Empty!"
                    : "Invalid tableName:" + tableName);
        }
    }

    public static final class InvalidColumnsCnt extends DingoClientException {
        private static final long serialVersionUID = 1L;

        public InvalidColumnsCnt(int realColumnCnt, int expectedColumnCnt) {
            super(ResultCode.PARAMETER_ERROR_INVALID_COLUMNCNT,
                "Invalid columns, realCnt=" + realColumnCnt + ", expectedCnt=" + expectedColumnCnt
            );
        }
    }

    public static final class InvalidUserKeyCnt extends DingoClientException {
        private static final long serialVersionUID = 1L;

        public InvalidUserKeyCnt(int realKeyCnt, int expectedKeyCnt) {
            super(ResultCode.PARAMETER_ERROR_INVALID_KEYCNT,
                "Invalid input userKeys, realCnt=" + realKeyCnt + ", expectedCnt=" + expectedKeyCnt
            );
        }
    }

    public static final class InvalidPrimaryKeyData extends DingoClientException {
        private static final long serialVersionUID = 1L;

        public InvalidPrimaryKeyData() {
            super(ResultCode.NULL_KEY_ERROR, "Invalid primary key, primary key is Empty");
        }
    }

}

