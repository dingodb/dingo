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

package io.dingodb.sdk.utils;

public final class ResultCode {
    /**
     * Max retries limit reached.
     */
    public static final int MAX_RETRIES_EXCEEDED = -1000;

    /**
     * Client serialization error.
     */
    public static final int SERIALIZE_ERROR = -1001;

    /**
     * Server is not accepting requests.
     */
    public static final int SERVER_NOT_AVAILABLE = -1002;

    /**
     * Max connections would be exceeded.  There are no more available connections.
     */
    public static final int NO_MORE_CONNECTIONS = -1003;

    /**
     * Client parse error.
     */
    public static final int PARSE_ERROR = -1004;

    /**
     * Generic client error.
     */
    public static final int CLIENT_ERROR = -1005;

    /**
     * Operation was successful.
     */
    public static final int OK = 0;

    /**
     * Unknown server failure.
     */
    public static final int SERVER_ERROR = 1000;

    /**
     * Partition is unavailable.
     */
    public static final int PARTITION_UNAVAILABLE = 1001;


    /**
     * Invalid input data. Start from 2000.
     * ====================================
     */

    /**
     * On retrieving, touching or replacing a record that doesn't exist.
     */
    public static final int KEY_NOT_FOUND_ERROR = 2001;


    /**
     * Invalid input key cnt.
     */
    public static final int PARAMETER_ERROR_INVALID_KEYCNT = 2002;

    /**
     * Invalid input column cnt.
     */
    public static final int PARAMETER_ERROR_INVALID_COLUMNCNT = 2003;

    /**
     * On create-only (write unique) operations on a record that already
     * exists.
     */
    public static final int KEY_EXISTS_ERROR = 2004;

    /**
     * Key type mismatch.
     */
    public static final int KEY_MISMATCH = 2005;

    /**
     * Invalid namespace.
     */
    public static final int INVALID_TABLE_NAME = 2006;

    /**
     * Invalid primary key.
     */
    public static final int NULL_KEY_ERROR = 2007;

    /**
     * Server Error. Start from 3000
     * ============================================
     */

    /**
     * Client or server has timed out.
     */
    public static final int TIMEOUT = 3000;

    /**
     * Return result code as a string.
     */
    public static String getResultString(int resultCode) {
        switch (resultCode) {
            case MAX_RETRIES_EXCEEDED:
                return "Max retries exceeded";

            case SERIALIZE_ERROR:
                return "Serialize error";

            case SERVER_NOT_AVAILABLE:
                return "Server not available";

            case NO_MORE_CONNECTIONS:
                return "No more available connections";

            case PARSE_ERROR:
                return "Parse error";

            case CLIENT_ERROR:
                return "Client error";

            case OK:
                return "ok";

            case SERVER_ERROR:
                return "Server error";

            case KEY_NOT_FOUND_ERROR:
                return "Key not found";

            case PARAMETER_ERROR_INVALID_KEYCNT:
                return "Input key count is not equal";

            case PARAMETER_ERROR_INVALID_COLUMNCNT:
                return "Input column count is not equal";

            case KEY_EXISTS_ERROR:
                return "Key already exists";

            case PARTITION_UNAVAILABLE:
                return "Partition unavailable";

            case TIMEOUT:
                return "Timeout";

            case KEY_MISMATCH:
                return "Key mismatch";

            case INVALID_TABLE_NAME:
                return "Namespace not found";

            case NULL_KEY_ERROR:
                return "Input primary key is null";

            default:
                return "";
        }
    }
}
