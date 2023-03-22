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

public class ExtendedClientCapabilities {


    public static final int CLIENT_MULTIPLE_STATEMENTS = 0x0001;

    public static final int CLIENT_MULTIPLE_RESULTS = 0x0002;

    public static final int CLIENT_PS_MULTIPLE_RESULTS = 0x0004;

    public static final int CLIENT_PLUGIN_AUTH = 0x0008;

    public static final int CLIENT_CONNECT_ATTRS = 0x0010;

    public static final int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x0020;

    public static final int CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS = 0x0040;

    public static final int CLIENT_SESSION_VARIABLE_TRACKING = 0x0080;

    public static final int CLIENT_DEPRECATE_EOF = 0x0100;

    public static final int CLIENT_CAN_HANDLE_OPTIONAL_RESULTSET_METADATA = 0x0200;

    public static final int CLIENT_ZSTD_COMPRESSION_ALGORITHM = 0x0400;

    public static final int CLIENT_QUERY_ATTRIBUTES = 0x0800;

    public static final int CLIENT_MULTIFACTOR_AUTHENTICATION = 0x1000;

    public static final int CLIENT_CAPABILITY_EXTENSION = 0x2000;

}
