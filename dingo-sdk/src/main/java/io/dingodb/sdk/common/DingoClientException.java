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
     * Exception thrown when Java serialization error occurs.
     */
    public static final class Serialize extends DingoClientException {
        private static final long serialVersionUID = 1L;

        public Serialize(Throwable ex) {
            super(ResultCode.SERIALIZE_ERROR, ex);
        }
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
    public static final class InvalidTableNames extends DingoClientException {
        private static final long serialVersionUID = 1L;

        public InvalidTableNames(String ns, int mapSize) {
            super(ResultCode.INVALID_NAMESPACE,
                (mapSize == 0) ? "Partition map empty" : "Namespace not found in partition map: " + ns);
        }
    }

}

