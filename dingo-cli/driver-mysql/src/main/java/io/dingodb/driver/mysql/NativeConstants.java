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

package io.dingodb.driver.mysql;

/**
 * Constants specific to legacy MySQL protocol.
 *
 */
public class NativeConstants {

    /** Maximum size of MySQL packet payload. */
    public static final int MAX_PACKET_SIZE = 256 * 256 * 256 - 1;
    /** Size of MySQL packet header (payload size + packet sequence ID). */
    public static final int HEADER_LENGTH = 4;
    public static final int SEED_LENGTH = 20;

    /* Type ids of response packets. */
    public static final short TYPE_ID_ERROR = 0xFF;
    public static final short TYPE_ID_EOF = 0xFE;
    public static final short TYPE_ID_LOCAL_INFILE = 0xFB;
    public static final short TYPE_ID_OK = 0;

    /* MySQL binary protocol value lengths. */
    public static final int BIN_LEN_INT1 = 1;
    public static final int BIN_LEN_INT2 = 2;
    public static final int BIN_LEN_INT4 = 4;
    public static final int BIN_LEN_INT8 = 8;
    public static final int BIN_LEN_FLOAT = 4;
    public static final int BIN_LEN_DOUBLE = 8;
    public static final int BIN_LEN_DATE = 4;
    public static final int BIN_LEN_TIMESTAMP_NO_FRAC = 7;
    public static final int BIN_LEN_TIMESTAMP_WITH_MICROS = 11;
    public static final int BIN_LEN_TIMESTAMP_WITH_TZ = 13;
    public static final int BIN_LEN_TIME_NO_FRAC = 8;
    public static final int BIN_LEN_TIME_WITH_MICROS = 12;

    /*
     * Command signatures
     */
    public static final int COM_SLEEP = 0;
    public static final int COM_QUIT = 1;
    public static final int COM_INIT_DB = 2;
    public static final int COM_QUERY = 3;
    public static final int COM_FIELD_LIST = 4; // Not used; deprecated in MySQL 5.7.11 and MySQL 8.0.0.
    public static final int COM_CREATE_DB = 5; // Not used; deprecated?
    public static final int COM_DROP_DB = 6; // Not used; deprecated?
    public static final int COM_REFRESH = 7; // Not used; deprecated in MySQL 5.7.11 and MySQL 8.0.0.
    public static final int COM_SHUTDOWN = 8; // Deprecated in MySQL 5.7.9 and MySQL 8.0.0.
    public static final int COM_STATISTICS = 9;
    public static final int COM_PROCESS_INFO = 10; // Not used; deprecated in MySQL 5.7.11 and MySQL 8.0.0.
    public static final int COM_CONNECT = 11;
    public static final int COM_PROCESS_KILL = 12; // Not used; deprecated in MySQL 5.7.11 and MySQL 8.0.0.
    public static final int COM_DEBUG = 13;
    public static final int COM_PING = 14;
    public static final int COM_TIME = 15;
    public static final int COM_DELAYED_INSERT = 16;
    public static final int COM_CHANGE_USER = 17;
    public static final int COM_BINLOG_DUMP = 18;
    public static final int COM_TABLE_DUMP = 19;
    public static final int COM_CONNECT_OUT = 20;
    public static final int COM_REGISTER_SLAVE = 21;
    public static final int COM_STMT_PREPARE = 22;
    public static final int COM_STMT_EXECUTE = 23;
    public static final int COM_STMT_SEND_LONG_DATA = 24;
    public static final int COM_STMT_CLOSE = 25;
    public static final int COM_STMT_RESET = 26;
    public static final int COM_SET_OPTION = 27;
    public static final int COM_STMT_FETCH = 28;
    public static final int COM_DAEMON = 29;
    public static final int COM_BINLOG_DUMP_GTID = 30;
    public static final int COM_RESET_CONNECTION = 31;

    /**
     * Used to indicate that the server sent no field-level character set information,
     * so the driver should use the connection-level character encoding instead.
     */
    public static final int NO_CHARSET_INFO = -1;

    /**
     * Basic protocol data types as they are defined in http://dev.mysql.com/doc/internals/en/integer.html
     *
     */
    public enum IntegerDataType {

        /**
         * 1 byte Protocol::FixedLengthInteger.
         */
        INT1,

        /**
         * 2 byte Protocol::FixedLengthInteger.
         */
        INT2,

        /**
         * 3 byte Protocol::FixedLengthInteger.
         */
        INT3,

        /**
         * 4 byte Protocol::FixedLengthInteger.
         */
        INT4,

        /**
         * 6 byte Protocol::FixedLengthInteger.
         */
        INT6,

        /**
         * 8 byte Protocol::FixedLengthInteger.
         */
        INT8,

        /**
         * Length-Encoded Integer Type.
         */
        INT_LENENC;
    }

    /**
     * Basic protocol data types as they are defined in http://dev.mysql.com/doc/internals/en/string.html
     * which require explicit length specification.
     *
     */
    public static enum StringLengthDataType {

        /**
         * Protocol::FixedLengthString.
         * Fixed-length strings have a known, hardcoded length.
         */
        STRING_FIXED,

        /**
         * Protocol::VariableLengthString.
         * The length of the string is determined by another field or is calculated at runtime
         */
        STRING_VAR;
    }

    /**
     * Basic self-describing protocol data types as they are defined in http://dev.mysql.com/doc/internals/en/string.html
     *
     */
    public static enum StringSelfDataType {

        /**
         * Protocol::NulTerminatedString
         * Strings that are terminated by a [00] byte.
         */
        STRING_TERM,

        /**
         * Protocol::LengthEncodedString
         * A length encoded string is a string that is prefixed with length encoded integer describing
         * the length of the string.
         * It is a special case of Protocol::VariableLengthString
         */
        STRING_LENENC,

        /**
         * Protocol::RestOfPacketString
         * If a string is the last component of a packet, its length can be calculated from the overall
         * packet length minus the current position.
         */
        STRING_EOF;
    }

    public NativeConstants() {
        super();
    }
}
