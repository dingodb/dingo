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

package io.dingodb.common.mysql.constant;

public class ServerStatus {
    public static final int SERVER_STATUS_IN_TRANS = 0x0001;
    public static final int SERVER_STATUS_AUTOCOMMIT = 0x0002;
    public static final int SERVER_MORE_RESULTS_EXISTS = 0x0008;
    public static final int SERVER_STATUS_NO_GOOD_INDEX_USED = 0x0010;
    public static final int SERVER_STATUS_NO_INDEX_USED = 0x0020;
    // Used by Binary Protocol Resultset to signal that COM_STMT_FETCH must be
    // used to fetch the row-data.
    public static final int SERVER_STATUS_CURSOR_EXISTS = 0x0040;
    public static final int SERVER_STATUS_LAST_ROW_SENT = 0x0080;
    public static final int SERVER_STATUS_DB_DROPPED = 0x0100;
    public static final int SERVER_STATUS_NO_BACKSLASH_ESCAPES = 0x0200;
    public static final int SERVER_STATUS_METADATA_CHANGED = 0x0400;
    public static final int SERVER_QUERY_WAS_SLOW = 0x0800;
    public static final int SERVER_PS_OUT_PARAMS = 0x1000;
    // in a read-only transaction
    public static final int SERVER_STATUS_IN_TRANS_READONLY = 0x2000;
    // connection state information has changed
    public static final int SERVER_SESSION_STATE_CHANGED = 0x4000;

}
