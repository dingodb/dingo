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

public enum CapabilityFlags {
    CLIENT_LONG_PASSWORD(0x00000001, "Use the improved version of Old Password Authentication. "
        + "Assumed to be set since 4.1.1."),
    CLIENT_FOUND_ROWS(0x00000002, "Send found rows instead of affected rows in EOF_Packet."),
    CLIENT_LONG_FLAG(0x00000004, "Longer flags in Protocol::ColumnDefinition320."
        + "Server Supports longer flags. Client Expects longer flags."),
    CLIENT_CONNECT_WITH_DB(0x00000008,
            "Database (schema) name can be specified on connect in Handshake Response Packet. "
                    + " Server Supports schema-name in Handshake Response Packet. "
                    + " Client Handshake Response Packet contains a schema-name."),
    CLIENT_NO_SCHEMA(0x00000010, "Server Do not permit database.table.column."),
    CLIENT_COMPRESS(0x00000020, "Compression protocol supported. Server Supports compression. "
            + "Client Switches to Compression compressed protocol after successful authentication."),
    CLIENT_ODBC(0x00000040, "Special handling of ODBC behavior. No special behavior since 3.22."),
    CLIENT_LOCAL_FILES(0x00000080, "Can use LOAD DATA LOCAL. Server Enables the LOCAL INFILE request "
        + "of LOAD DATA|XML."
            + " Client Will handle LOCAL INFILE request."),
    CLIENT_IGNORE_SPACE(0x00000100, "Server Parser can ignore spaces before '('. "
            + " Client Let the parser ignore spaces before '('."),
    CLIENT_PROTOCOL_41(0x00000200, "Server Supports the 4.1 protocol. Client Uses the 4.1 protocol."
            + " this value was CLIENT_CHANGE_USER in 3.22, unused in 4.0"),
    CLIENT_INTERACTIVE(0x00000400, "wait_timeout versus wait_interactive_timeout. "
            + "Server Supports interactive and noninteractive clients. Client is interactive. "
            + "See mysql_real_connect()"),
    CLIENT_SSL(0x00000800, "Server Supports SSL. Client Switch to SSL after sending the capabilities-flags."),
    CLIENT_IGNORE_SIGPIPE(0x00001000, "Client Do not issue SIGPIPE if network failures occur "
        + "(libmysqlclient only)."
            + " See mysql_real_connect()"),
    CLIENT_TRANSACTIONS(0x00002000, "Server Can send status flags in EOF_Packet. "
        + "Client Expects status flags in EOF_Packet."
            + " This flag is optional in 3.23, but always set by the server since 4.0."),
    CLIENT_RESERVED(0x00004000, "Unused. Was named CLIENT_PROTOCOL_41 in 4.1.0."),
    CLIENT_SECURE_CONNECTION(0x00008000, "Server Supports Authentication::Native41. "
        + "Client Supports Authentication::Native41."),
    CLIENT_MULTI_STATEMENTS(0x00010000, "Server Can handle multiple statements per COM_QUERY and COM_STMT_PREPARE."
            + " Client May send multiple statements per COM_QUERY and COM_STMT_PREPARE. "
            + "Was named CLIENT_MULTI_QUERIES in 4.1.0, renamed later. Requires CLIENT_PROTOCOL_41"),
    CLIENT_MULTI_RESULTS(0x00020000, "Server Can send multiple resultsets for COM_QUERY."
            + " Client Can handle multiple resultsets for COM_QUERY. Requires CLIENT_PROTOCOL_41"),
    CLIENT_PS_MULTI_RESULTS(0x00040000, "Server Can send multiple resultsets for COM_STMT_EXECUTE."
            + " Client Can handle multiple resultsets for COM_STMT_EXECUTE. Requires CLIENT_PROTOCOL_41"),
    CLIENT_PLUGIN_AUTH(0x00080000, "Server Sends extra data in Initial Handshake Packet and supports the pluggable "
            + "authentication protocol. Client Supports authentication plugins. Requires CLIENT_PROTOCOL_41"),
    CLIENT_CONNECT_ATTRS(0x00100000, "Server Permits connection attributes in Protocol::HandshakeResponse41."
            + " Client Sends connection attributes in Protocol::HandshakeResponse41."),
    CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA(0x00200000, "Server Understands length-encoded integer "
        + "for auth response data "
            + "in Protocol::HandshakeResponse41. Client Length of auth response data in Protocol::HandshakeResponse41"
            + " is a length-encoded integer. The flag was introduced in 5.6.6, but had the wrong value."),
    CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS(0x00400000, "Server Announces support for expired password extension. "
            + "Client Can handle expired passwords."),
    CLIENT_SESSION_TRACK(0x00800000, "Server Can set SERVER_SESSION_STATE_CHANGED in the Status Flags and "
            + "send session-state change data after a OK packet. "
            + "Client Expects the server to send sesson-state changes after a OK packet."),
    CLIENT_DEPRECATE_EOF(0x01000000, "Server Can send OK after a Text Resultset."
            + "Client Expects an OK (instead of EOF) after the resultset rows of a Text Resultset."),;


    /**
     * code.
     */
    private final int code;
    /**
     * desc.
     */
    private final String desc;

    CapabilityFlags(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
