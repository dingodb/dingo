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

package io.dingodb.driver.mysql.packet;

import io.dingodb.common.mysql.MysqlMessage;
import io.dingodb.driver.mysql.MysqlType;
import io.dingodb.driver.mysql.NativeConstants;
import io.dingodb.driver.mysql.util.BufferUtil;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class ExecuteStatementPacket extends MysqlPacket {

    private final int paramCount;
    public byte flag;

    public int statementId;

    public byte flags;

    public long iterations;

    public String nullBitMap;
    public byte newParamBoundFlag;

    public Integer[] types;

    public Map<Integer, TypeValue> paramValMap = new LinkedHashMap<>();

    public ExecuteStatementPacket(int paramCount, Integer[] types) {
        this.paramCount = paramCount;
        this.types = types;
    }

    @Override
    public int calcPacketSize() {
        // As a server, there is no need to send this packet, so there is no need to calculate the packet size
        return 0;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Execute Statement Packet";
    }

    @Override
    public void read(byte[] data) {
        MysqlMessage message = new MysqlMessage(data);
        packetId = message.read();
        flag = message.read();
        statementId = message.readInt();
        flags = message.read();
        iterations = message.readUB4();
        int nullMapNum = (paramCount + 7) / 8;
        StringBuilder nullBitmapBuilder = new StringBuilder();
        for (int i = 0; i < nullMapNum; i ++) {
            nullBitmapBuilder.append(BufferUtil.getBinaryStrFromByte(message.read()));
        }
        nullBitMap = nullBitmapBuilder.toString();
        if (!message.hasRemaining()) {
            return;
        }

        newParamBoundFlag = message.read();
        Integer[] types = null;
        if (newParamBoundFlag == 0x01) {
            types = new Integer[paramCount];
            for (int i = 0; i < paramCount; i ++) {
                types[i] = message.read() & 0xff;
                // unsigned
                message.read();
            }
            this.types = types;
        } else if (this.types != null) {
            types = this.types;
        }

        int bitmapLength = nullBitMap.length();
        int length;
        boolean isBlob;
        int typeSize = Objects.requireNonNull(types).length;
        for (int i = 1; i <= typeSize; i ++) {
            int type = types[i - 1];
            isBlob = false;
            switch (type) {
                case MysqlType.FIELD_TYPE_TINY:
                    length = NativeConstants.BIN_LEN_INT1;
                    break;
                case MysqlType.FIELD_TYPE_SHORT:
                    length = NativeConstants.BIN_LEN_INT2;
                    break;
                case MysqlType.FIELD_TYPE_LONG:
                    length = NativeConstants.BIN_LEN_INT4;
                    break;
                case MysqlType.FIELD_TYPE_LONGLONG:
                    length = NativeConstants.BIN_LEN_INT8;
                    break;
                case MysqlType.FIELD_TYPE_FLOAT:
                    length = NativeConstants.BIN_LEN_FLOAT;
                    break;
                case MysqlType.FIELD_TYPE_DOUBLE:
                    length = NativeConstants.BIN_LEN_DOUBLE;
                    break;
                case MysqlType.FIELD_TYPE_DATE:
                case MysqlType.FIELD_TYPE_TIME:
                case MysqlType.FIELD_TYPE_DATETIME:
                case MysqlType.FIELD_TYPE_TIMESTAMP:
                case MysqlType.FIELD_TYPE_VAR_STRING:
                case MysqlType.FIELD_TYPE_STRING:
                case MysqlType.FIELD_TYPE_VARCHAR:
                case MysqlType.FIELD_TYPE_DECIMAL:
                case MysqlType.FIELD_TYPE_NEWDECIMAL:
                    length = message.read() & 0xff;
                    break;
                case MysqlType.FIELD_TYPE_BLOB:
                case MysqlType.FIELD_TYPE_TINY_BLOB:
                case MysqlType.FIELD_TYPE_MEDIUM_BLOB:
                case MysqlType.FIELD_TYPE_LONG_BLOB:
                    length = 0;
                    isBlob = true;
                    break;
                default:
                    length = 0;
                    break;
            }
            if (nullBitMap.charAt(bitmapLength - i) == '1') {
                paramValMap.put(i, new TypeValue(type, new byte[0]));
            } else {
                if (!isBlob) {
                    byte[] bytes = message.readBytes(length);
                    paramValMap.put(i, new TypeValue(type, bytes));
                }
            }
        }
    }

    @Override
    public void write(ByteBuf buffer) {

    }

    @Getter
    @Setter
    public static class TypeValue {
        int type;
        byte[] value;

        public TypeValue(int type, byte[] value) {
            this.type = type;
            this.value = value;
        }
    }
}
