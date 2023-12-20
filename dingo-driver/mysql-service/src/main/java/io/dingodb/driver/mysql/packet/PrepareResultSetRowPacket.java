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

import io.dingodb.driver.mysql.MysqlConnection;
import io.dingodb.driver.mysql.util.BufferUtil;
import io.netty.buffer.ByteBuf;
import lombok.Setter;

import java.io.UnsupportedEncodingException;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static io.dingodb.driver.mysql.command.MysqlResponseHandler.getArrayObject;

public class PrepareResultSetRowPacket extends MysqlPacket {
    List<Object> values = new ArrayList<>();

    @Setter
    private String characterSet;

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
                Object val = values.get(i - 1);
                if (val == null) {
                    continue;
                }
                switch (typeName) {
                    case "INTEGER":
                    case "FLOAT":
                        totalSize += 4;
                        break;
                    case "BIGINT":
                    case "DOUBLE":
                        totalSize += 8;
                        break;
                    case "DATE":
                        // length + date(4)
                        totalSize += 1 + 4;
                        break;
                    case "DATETIME":
                    case "TIMESTAMP":
                        totalSize += 11 + 1;
                        break;
                    case "TIME":
                        totalSize += 12 + 1;
                        break;
                    case "BOOLEAN":
                        totalSize += 1;
                        break;
                    case "VARCHAR":
                    case "CHAR":
                    case "ARRAY":
                    case "MULTISET":
                        byte[] v;
                        try {
                            v = val.toString().getBytes(characterSet);
                        } catch (UnsupportedEncodingException e) {
                            throw new RuntimeException(e);
                        }
                        values.set(i - 1, v);
                        totalSize += BufferUtil.getLength(v);
                        break;
                    case "VARBINARY":
                        byte[] blob = (byte[]) val;
                        totalSize += BufferUtil.getLength(blob);
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
                            if (val instanceof Double) {
                                val = ((Double) val).floatValue();
                            }
                            BufferUtil.writeFloat(buffer, (float) val);
                            break;
                        case "BIGINT":
                            BufferUtil.writeLong(buffer, (Long) val);
                            break;
                        case "DOUBLE":
                            BufferUtil.writeDouble(buffer, (Double) val);
                            break;
                        case "DATE":
                            BufferUtil.writeLength(buffer, 4);
                            BufferUtil.writeDate(buffer, (Date) val);
                            break;
                        case "DATETIME":
                        case "TIMESTAMP":
                            BufferUtil.writeLength(buffer, 11);
                            BufferUtil.writeDateTime(buffer, (Timestamp) val);
                            break;
                        case "TIME":
                            BufferUtil.writeLength(buffer, 12);
                            BufferUtil.writeTime(buffer, (Time) val);
                            break;
                        case "BOOLEAN":
                            Boolean valBool = (Boolean) val;
                            if (valBool) {
                                buffer.writeByte(1);
                            } else {
                                buffer.writeByte(0);
                            }
                            break;
                        case "VARCHAR":
                        case "CHAR":
                        case "ARRAY":
                        case "MULTISET":
                        case "VARBINARY":
                            byte[] v = (byte[]) val;
                            BufferUtil.writeLength(buffer, v.length);
                            buffer.writeBytes(v);
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

    public void addColumnValue(Object val, MysqlConnection connection) throws SQLException {
        if (val instanceof Array) {
            val = getArrayObject(connection, val);
        }
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
