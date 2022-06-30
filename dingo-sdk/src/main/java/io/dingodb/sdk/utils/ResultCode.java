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
     * Max errors limit reached.
     */
    public static final int MAX_ERROR_RATE = -12;

    /**
     * Max retries limit reached.
     */
    public static final int MAX_RETRIES_EXCEEDED = -11;

    /**
     * Client serialization error.
     */
    public static final int SERIALIZE_ERROR = -10;

    /**
     * Async delay queue is full.
     */
    public static final int ASYNC_QUEUE_FULL = -9;

    /**
     * Server is not accepting requests.
     */
    public static final int SERVER_NOT_AVAILABLE = -8;

    /**
     * Max connections would be exceeded.  There are no more available connections.
     */
    public static final int NO_MORE_CONNECTIONS = -7;

    /**
     * Query was terminated by user.
     */
    public static final int QUERY_TERMINATED = -5;

    /**
     * Scan was terminated by user.
     */
    public static final int SCAN_TERMINATED = -4;

    /**
     * Chosen node is not currently active.
     */
    public static final int INVALID_NODE_ERROR = -3;

    /**
     * Client parse error.
     */
    public static final int PARSE_ERROR = -2;

    /**
     * Generic client error.
     */
    public static final int CLIENT_ERROR = -1;

    /**
     * Operation was successful.
     */
    public static final int OK = 0;

    /**
     * Unknown server failure.
     */
    public static final int SERVER_ERROR = 1;

    /**
     * On retrieving, touching or replacing a record that doesn't exist.
     */
    public static final int KEY_NOT_FOUND_ERROR = 2;

    /**
     * On modifying a record with unexpected generation.
     */
    public static final int GENERATION_ERROR = 3;

    /**
     * Bad parameter(s) were passed in database operation call.
     */
    public static final int PARAMETER_ERROR = 4;

    /**
     * On create-only (write unique) operations on a record that already
     * exists.
     */
    public static final int KEY_EXISTS_ERROR = 5;

    /**
     * Bin already exists on a create-only operation.
     */
    public static final int BIN_EXISTS_ERROR = 6;

    /**
     * Expected cluster was not received.
     */
    public static final int CLUSTER_KEY_MISMATCH = 7;

    /**
     * Server has run out of memory.
     */
    public static final int SERVER_MEM_ERROR = 8;

    /**
     * Client or server has timed out.
     */
    public static final int TIMEOUT = 9;

    /**
     * Operation not allowed in current configuration.
     */
    public static final int ALWAYS_FORBIDDEN = 10;

    /**
     * Partition is unavailable.
     */
    public static final int PARTITION_UNAVAILABLE = 11;

    /**
     * Operation is not supported with configured bin type (single-bin or
     * multi-bin).
     */
    public static final int BIN_TYPE_ERROR = 12;

    /**
     * Record size exceeds limit.
     */
    public static final int RECORD_TOO_BIG = 13;

    /**
     * Too many concurrent operations on the same record.
     */
    public static final int KEY_BUSY = 14;

    /**
     * Scan aborted by server.
     */
    public static final int SCAN_ABORT = 15;

    /**
     * Unsupported Server Feature (e.g. Scan + UDF)
     */
    public static final int UNSUPPORTED_FEATURE = 16;

    /**
     * Bin not found on update-only operation.
     */
    public static final int BIN_NOT_FOUND = 17;

    /**
     * Device not keeping up with writes.
     */
    public static final int DEVICE_OVERLOAD = 18;

    /**
     * Key type mismatch.
     */
    public static final int KEY_MISMATCH = 19;

    /**
     * Invalid namespace.
     */
    public static final int INVALID_NAMESPACE = 20;

    /**
     * Bin name length greater than 14 characters or maximum bins exceeded.
     */
    public static final int BIN_NAME_TOO_LONG = 21;

    /**
     * Operation not allowed at this time.
     */
    public static final int FAIL_FORBIDDEN = 22;

    /**
     * Map element not found in UPDATE_ONLY write mode.
     */
    public static final int ELEMENT_NOT_FOUND = 23;

    /**
     * Map element exists in CREATE_ONLY write mode.
     */
    public static final int ELEMENT_EXISTS = 24;

    /**
     * Attempt to use an Enterprise feature on a Community server or a server
     * without the applicable feature key.
     */
    public static final int ENTERPRISE_ONLY = 25;

    /**
     * The operation cannot be applied to the current bin value on the server.
     */
    public static final int OP_NOT_APPLICABLE = 26;

    /**
     * The transaction was not performed because the filter was false.
     */
    public static final int FILTERED_OUT = 27;

    /**
     * Write command loses conflict to XDR.
     */
    public static final int LOST_CONFLICT = 28;

    /**
     * There are no more records left for query.
     */
    public static final int QUERY_END = 50;

    /**
     * Security functionality not supported by connected server.
     */
    public static final int SECURITY_NOT_SUPPORTED = 51;

    /**
     * Security functionality not enabled by connected server.
     */
    public static final int SECURITY_NOT_ENABLED = 52;

    /**
     * Security type not supported by connected server.
     */
    public static final int SECURITY_SCHEME_NOT_SUPPORTED = 53;

    /**
     * Administration command is invalid.
     */
    public static final int INVALID_COMMAND = 54;

    /**
     * Administration field is invalid.
     */
    public static final int INVALID_FIELD = 55;

    /**
     * Security protocol not followed.
     */
    public static final int ILLEGAL_STATE = 56;

    /**
     * User name is invalid.
     */
    public static final int INVALID_USER = 60;

    /**
     * User was previously created.
     */
    public static final int USER_ALREADY_EXISTS = 61;

    /**
     * Password is invalid.
     */
    public static final int INVALID_PASSWORD = 62;

    /**
     * Password has expired.
     */
    public static final int EXPIRED_PASSWORD = 63;

    /**
     * Forbidden password (e.g. recently used)
     */
    public static final int FORBIDDEN_PASSWORD = 64;

    /**
     * Security credential is invalid.
     */
    public static final int INVALID_CREDENTIAL = 65;

    /**
     * Login session expired.
     */
    public static final int EXPIRED_SESSION = 66;

    /**
     * Role name is invalid.
     */
    public static final int INVALID_ROLE = 70;

    /**
     * Role already exists.
     */
    public static final int ROLE_ALREADY_EXISTS = 71;

    /**
     * Privilege is invalid.
     */
    public static final int INVALID_PRIVILEGE = 72;

    /**
     * Invalid IP address whitelist.
     */
    public static final int INVALID_WHITELIST = 73;

    /**
     * Quotas not enabled on server.
     */
    public static final int QUOTAS_NOT_ENABLED = 74;

    /**
     * Invalid quota value.
     */
    public static final int INVALID_QUOTA = 75;

    /**
     * User must be authentication before performing database operations.
     */
    public static final int NOT_AUTHENTICATED = 80;

    /**
     * User does not possess the required role to perform the database operation.
     */
    public static final int ROLE_VIOLATION = 81;

    /**
     * Command not allowed because sender IP address not whitelisted.
     */
    public static final int NOT_WHITELISTED = 82;

    /**
     * Quota exceeded.
     */
    public static final int QUOTA_EXCEEDED = 83;

    /**
     * A user defined function returned an error code.
     */
    public static final int UDF_BAD_RESPONSE = 100;

    /**
     * Batch functionality has been disabled.
     */
    public static final int BATCH_DISABLED = 150;

    /**
     * Batch max requests have been exceeded.
     */
    public static final int BATCH_MAX_REQUESTS_EXCEEDED = 151;

    /**
     * All batch queues are full.
     */
    public static final int BATCH_QUEUES_FULL = 152;

    /**
     * Secondary index already exists.
     */
    public static final int INDEX_ALREADY_EXISTS = 200;
    public static final int INDEX_FOUND = 200;  // For legacy reasons.

    /**
     * Requested secondary index does not exist.
     */
    public static final int INDEX_NOTFOUND = 201;

    /**
     * Secondary index memory space exceeded.
     */
    public static final int INDEX_OOM = 202;

    /**
     * Secondary index not available.
     */
    public static final int INDEX_NOTREADABLE = 203;

    /**
     * Generic secondary index error.
     */
    public static final int INDEX_GENERIC = 204;

    /**
     * Index name maximum length exceeded.
     */
    public static final int INDEX_NAME_MAXLEN = 205;

    /**
     * Maximum number of indicies exceeded.
     */
    public static final int INDEX_MAXCOUNT = 206;

    /**
     * Secondary index query aborted.
     */
    public static final int QUERY_ABORTED = 210;

    /**
     * Secondary index queue full.
     */
    public static final int QUERY_QUEUEFULL = 211;

    /**
     * Secondary index query timed out on server.
     */
    public static final int QUERY_TIMEOUT = 212;

    /**
     * Generic query error.
     */
    public static final int QUERY_GENERIC = 213;

    /**
     * Should connection be put back into pool.
     */
    public static boolean keepConnection(int resultCode) {
        if (resultCode <= 0) {
            // Do not keep connection on client errors.
            return false;
        }

        switch (resultCode) {
            case SCAN_ABORT:
            case QUERY_ABORTED:
                return false;

            default:
                // Keep connection on TIMEOUT because it can be server response which does not
                // close socket.  Also, client timeout code path does not call this method.
                return true;
        }
    }

    /**
     * Return result code as a string.
     */
    public static String getResultString(int resultCode) {
        switch (resultCode) {

            case MAX_ERROR_RATE:
                return "Max error rate exceeded";

            case MAX_RETRIES_EXCEEDED:
                return "Max retries exceeded";

            case SERIALIZE_ERROR:
                return "Serialize error";

            case ASYNC_QUEUE_FULL:
                return "Async delay queue is full";

            case SERVER_NOT_AVAILABLE:
                return "Server not available";

            case NO_MORE_CONNECTIONS:
                return "No more available connections";

            case QUERY_TERMINATED:
                return "Query terminated";

            case SCAN_TERMINATED:
                return "Scan terminated";

            case INVALID_NODE_ERROR:
                return "Invalid node";

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

            case GENERATION_ERROR:
                return "Generation error";

            case PARAMETER_ERROR:
                return "Parameter error";

            case KEY_EXISTS_ERROR:
                return "Key already exists";

            case BIN_EXISTS_ERROR:
                return "Bin already exists";

            case CLUSTER_KEY_MISMATCH:
                return "Cluster key mismatch";

            case SERVER_MEM_ERROR:
                return "Server memory error";

            case TIMEOUT:
                return "Timeout";

            case ALWAYS_FORBIDDEN:
                return "Operation not allowed";

            case PARTITION_UNAVAILABLE:
                return "Partition unavailable";

            case BIN_TYPE_ERROR:
                return "Bin type error";

            case RECORD_TOO_BIG:
                return "Record too big";

            case KEY_BUSY:
                return "Hot key";

            case SCAN_ABORT:
                return "Scan aborted";

            case UNSUPPORTED_FEATURE:
                return "Unsupported Server Feature";

            case BIN_NOT_FOUND:
                return "Bin not found";

            case DEVICE_OVERLOAD:
                return "Device overload";

            case KEY_MISMATCH:
                return "Key mismatch";

            case INVALID_NAMESPACE:
                return "Namespace not found";

            case BIN_NAME_TOO_LONG:
                return "Bin name length greater than 14 characters or maximum bins exceeded";

            case FAIL_FORBIDDEN:
                return "Operation not allowed at this time";

            case ELEMENT_NOT_FOUND:
                return "Map key not found";

            case ELEMENT_EXISTS:
                return "Map key exists";

            case ENTERPRISE_ONLY:
                return "Enterprise only";

            case OP_NOT_APPLICABLE:
                return "Operation not applicable";

            case FILTERED_OUT:
                return "Transaction filtered out";

            case LOST_CONFLICT:
                return "Transaction failed due to conflict with XDR";

            case QUERY_END:
                return "Query end";

            case SECURITY_NOT_SUPPORTED:
                return "Security not supported";

            case SECURITY_NOT_ENABLED:
                return "Security not enabled";

            case SECURITY_SCHEME_NOT_SUPPORTED:
                return "Security scheme not supported";

            case INVALID_COMMAND:
                return "Invalid command";

            case INVALID_FIELD:
                return "Invalid field";

            case ILLEGAL_STATE:
                return "Illegal state";

            case INVALID_USER:
                return "Invalid user";

            case USER_ALREADY_EXISTS:
                return "User already exists";

            case INVALID_PASSWORD:
                return "Invalid password";

            case EXPIRED_PASSWORD:
                return "Password expired";

            case FORBIDDEN_PASSWORD:
                return "Password can't be reused";

            case INVALID_CREDENTIAL:
                return "Invalid credential";

            case EXPIRED_SESSION:
                return "Login session expired";

            case INVALID_ROLE:
                return "Invalid role";

            case ROLE_ALREADY_EXISTS:
                return "Role already exists";

            case INVALID_PRIVILEGE:
                return "Invalid privilege";

            case INVALID_WHITELIST:
                return "Invalid whitelist";

            case QUOTAS_NOT_ENABLED:
                return "Quotas not enabled";

            case INVALID_QUOTA:
                return "Invalid quota";

            case NOT_AUTHENTICATED:
                return "Not authenticated";

            case ROLE_VIOLATION:
                return "Role violation";

            case NOT_WHITELISTED:
                return "Command not whitelisted";

            case QUOTA_EXCEEDED:
                return "Quota exceeded";

            case UDF_BAD_RESPONSE:
                return "UDF returned error";

            case BATCH_DISABLED:
                return "Batch functionality has been disabled";

            case BATCH_MAX_REQUESTS_EXCEEDED:
                return "Batch max requests have been exceeded";

            case BATCH_QUEUES_FULL:
                return "All batch queues are full";

            case INDEX_ALREADY_EXISTS:
                return "Index already exists";

            case INDEX_NOTFOUND:
                return "Index not found";

            case INDEX_OOM:
                return "Index out of memory";

            case INDEX_NOTREADABLE:
                return "Index not readable";

            case INDEX_GENERIC:
                return "Index error";

            case INDEX_NAME_MAXLEN:
                return "Index name max length exceeded";

            case INDEX_MAXCOUNT:
                return "Index count exceeds max";

            case QUERY_ABORTED:
                return "Query aborted";

            case QUERY_QUEUEFULL:
                return "Query queue full";

            case QUERY_TIMEOUT:
                return "Query timeout";

            case QUERY_GENERIC:
                return "Query error";

            default:
                return "";
        }
    }
}
