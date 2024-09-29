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

import io.dingodb.common.mysql.CapabilityFlags;
import io.dingodb.common.mysql.MysqlMessage;
import io.dingodb.common.mysql.constant.ErrorCode;
import io.dingodb.driver.mysql.NativeConstants;
import io.dingodb.driver.mysql.util.BufferUtil;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;

@Slf4j
public class ERRPacket extends MysqlPacket {

    public int header;

    public int errorCode;

    public byte sqlStateMarker;

    public String sqlState;

    public String errorMessage;

    public int capabilities;

    public String characterSet;

    private byte[] errMsgBytes;

    @Override
    public void read(byte[] data) {
        MysqlMessage message = new MysqlMessage(data);
        //this.packetLength = message.readUB3();
        this.packetId = message.read();
        this.header = message.read();
        this.errorCode = message.readUB2();
        if ((capabilities & CapabilityFlags.CLIENT_PROTOCOL_41.getCode()) > 0) {
            sqlStateMarker = message.read();
            sqlState = new String(message.readBytes(5));
        }
        errorMessage = message.readString();
    }

    public static String encodeResponseError(String src) {
        if(src == null) {
            return null;
        }

        StringBuilder builder = new StringBuilder();
        for(char c : src.toCharArray()) {
            if(c > 0x7f)  {
                //deal with chinese character.
                builder.append("\\u").append(String.format("%04x", (int)c));
            } else {
                builder.append(c);
            }
        }
        return builder.toString();
    }

    @Override
    public void write(ByteBuf buffer) {
        byte[] stringBytes = null;

        if (errorMessage != null) {
            if(characterSet != null) {
                try {
                    errMsgBytes = errorMessage.getBytes(characterSet);
                    stringBytes = errMsgBytes;
                } catch (UnsupportedEncodingException e) {
                    log.error("Error message encoding Exception:{}", e.toString());
                    stringBytes = encodeResponseError(errorMessage).getBytes();
                }
            } else {
                stringBytes = errorMessage.getBytes();
            }
        }

        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.writeByte(packetId);
        buffer.writeByte(NativeConstants.TYPE_ID_ERROR);
        BufferUtil.writeUB2(buffer, errorCode);
        if ((capabilities & CapabilityFlags.CLIENT_PROTOCOL_41.getCode()) > 0) {
            buffer.writeByte((byte) '#');
            if (StringUtils.isBlank(sqlState)) {
                sqlState = ErrorCode.ER_NO.sqlState;
            }
            buffer.writeBytes(sqlState.getBytes());
        }

        if(stringBytes != null) {
            buffer.writeBytes(stringBytes);
        }
    }

    @Override
    public int calcPacketSize() {
        int size = 9;
        if (errorMessage != null) {
            if(characterSet != null && errMsgBytes != null) {
                size += errMsgBytes.length;
            } else {
                size += errorMessage.length();
            }
        }
        return size;
    }

    @Override
    public String getPacketInfo() {
        return "MySQL Error Packet";
    }

    @Override
    public String toString() {
        return "ERRPacket{"
                + "packetLength=" + packetLength
                + ", packetSequenceId=" + packetId
                + ", header=" + header
                + ", errorCode=" + errorCode
                + ", sqlStateMarker=" + sqlStateMarker
                + ", sqlState=" + sqlState
                + ", errorMessage='" + errorMessage + '\''
                + ", capabilities=" + capabilities
                + "}\n";
    }
}
