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

import io.dingodb.driver.mysql.util.BufferUtil;
import io.netty.buffer.ByteBuf;
import lombok.Setter;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PrepareResultSetRowPacket extends MysqlPacket {
    List<Object> values = new ArrayList<>();

    @Setter
    private ResultSetMetaData metaData;

    @Override
    public int calcPacketSize() {
        try {
            int totalSize = 0;
            // ok code
            totalSize ++;
            int columnCount = metaData.getColumnCount();
            totalSize += getMaskNullLength(columnCount);
            String typeName;
            for (int i = 1; i <= columnCount; i++) {
                typeName = metaData.getColumnTypeName(i);
                switch (typeName) {
                    case "INTEGER":
                    case "FLOAT":
                        totalSize += 4;
                        break;
                    case "BIGINT":
                    case "DOUBLE":
                        totalSize += 8;
                        break;
                    case "VARCHAR":
                    case "DATE":
                    case "DATETIME":
                    case "TIMESTAMP":
                    case "TIME":
                    case "CHAR":
                    case "ARRAY":
                    case "MULTISET":
                        Object val = values.get(i - 1);
                        if (val != null) {
                            byte[] v = val.toString().getBytes();
                            values.set(i - 1, v);
                            totalSize += BufferUtil.getLength(v);
                        }
                        break;
                    default:
                        break;
                }
            }
            return totalSize;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    protected String getPacketInfo() {
        return null;
    }

    @Override
    public void read(byte[] data) {

    }

    @Override
    public void write(ByteBuf buffer) {
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.writeByte(packetId);
        buffer.writeByte(OKPacket.HEADER);
        buffer.writeBytes(getMaskNullBuffer(values));
        try {
            String typeName;
            for (int i = 0; i < values.size(); i++) {
                Object val = values.get(i);
                if (val != null) {
                    typeName = metaData.getColumnTypeName(i + 1);
                    switch (typeName) {
                        case "INTEGER":
                            BufferUtil.writeInt(buffer, (Integer) val);
                            break;
                        case "FLOAT":
                            BufferUtil.writeFloat(buffer, (Float) val);
                            break;
                        case "BIGINT":
                            BufferUtil.writeLong(buffer, (Long) val);
                            break;
                        case "DOUBLE":
                            BufferUtil.writeDouble(buffer, (Double) val);
                            break;
                        case "VARCHAR":
                        case "DATE":
                        case "DATETIME":
                        case "TIMESTAMP":
                        case "TIME":
                        case "CHAR":
                        case "ARRAY":
                        case "MULTISET":
                            buffer.writeBytes((byte[]) val);
                            break;
                        default:
                            break;
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void addColumnValue(Object val) {
        values.add(val);
    }

    public static byte[] getMaskNullBuffer(List<Object> values) {
        int length = getMaskNullLength(values.size());
        int[] markNull = new int[length];

        for (int i = 0; i < values.size(); i ++) {
            if (values.get(i) == null) {
                int index = getMaskNullIndex(i);
                markNull[index] |= getMaskNullPos(i + 1, index);
            }
        }
        byte[] buffer = new byte[length];
        for (int i = 0; i < length; i ++) {
            buffer[i] = (byte) markNull[i];
        }
        return buffer;
    }

    public static int getMaskNullLength(int lengthCount) {
        return  (lengthCount + 9) / 8;
    }

    public static int getMaskNullIndex(int pos) {
        if (pos <= 6) {
            return 0;
        }
        if ((pos - 6) % 8 == 0) {
            return (pos - 6) / 8;
        }
        return  ((pos - 6) / 8) + 1;
    }

    public static int getMaskNullPos(int index, int pos) {
        if (pos == 0) {
            return (int) Math.pow(2, index + 1);
        } else {
            int tmp;
            if (pos == 1) {
                tmp = 6;
            } else {
                tmp = 6 + (pos - 1) * 8;
            }

            int result;
            index = index - tmp;
            if (index == 1) {
                result = 1;
            } else {
                result = (int) Math.pow(2, index - 1);
            }
            return result;
        }
    }

}
