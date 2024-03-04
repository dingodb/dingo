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

public class MysqlServer {

    public static final int getServerCapabilities() {
        int flag = 0;
        //lower
        flag |= CapabilityFlags.CLIENT_LONG_PASSWORD.getCode();
        flag |= CapabilityFlags.CLIENT_FOUND_ROWS.getCode();
        flag |= CapabilityFlags.CLIENT_LONG_FLAG.getCode();
        flag |= CapabilityFlags.CLIENT_CONNECT_WITH_DB.getCode();
        //flag |= CapabilityFlags.CLIENT_ODBC.getCode();
        flag |= CapabilityFlags.CLIENT_IGNORE_SPACE.getCode();
        flag |= CapabilityFlags.CLIENT_PROTOCOL_41.getCode();
        flag |= CapabilityFlags.CLIENT_INTERACTIVE.getCode();
        flag |= CapabilityFlags.CLIENT_IGNORE_SIGPIPE.getCode();
        flag |= CapabilityFlags.CLIENT_TRANSACTIONS.getCode();
        flag |= CapabilityFlags.CLIENT_SECURE_CONNECTION.getCode();
        //flag |= CapabilityFlags.CLIENT_SSL.getCode();
        //upper
        //flag |= CapabilityFlags.CLIENT_MULTI_STATEMENTS.getCode();
        flag |= CapabilityFlags.CLIENT_MULTI_RESULTS.getCode();
        flag |= CapabilityFlags.CLIENT_PS_MULTI_RESULTS.getCode();
        //flag |= CapabilityFlags.CLIENT_PLUGIN_AUTH.getCode();
        flag |= CapabilityFlags.CLIENT_CONNECT_ATTRS.getCode();
        flag |= CapabilityFlags.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA.getCode();
        flag |= CapabilityFlags.CLIENT_SESSION_TRACK.getCode();
        return flag;
    }

}
