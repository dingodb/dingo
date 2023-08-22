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

import io.dingodb.common.mysql.ExtendedClientCapabilities;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class ResultSetPacket extends MysqlPacket {

    public ColumnsNumberPacket columnsNumber;

    public List<ColumnPacket> columns = new ArrayList<>();

    public EOFPacket columnsEof;

    public List<MysqlPacket> rows = new ArrayList<>();

    public EOFPacket rowsEof;

    public OKPacket okPacket;

    public int capabilities;

    public int extendClientCapabilities;


    @Override
    public void read(byte[] data) {

    }

    @Override
    public void write(ByteBuf buffer) {
        columnsNumber.write(buffer);
        for (ColumnPacket columnPacket : columns) {
            columnPacket.write(buffer);
        }
        int deprecateEof = extendClientCapabilities & ExtendedClientCapabilities.CLIENT_DEPRECATE_EOF;
        if (deprecateEof != 0) {
            for (MysqlPacket mysqlPacket : rows) {
                mysqlPacket.write(buffer);
            }
            okPacket.write(buffer);
        } else {
            columnsEof.write(buffer);
            for (MysqlPacket mysqlPacket : rows) {
                mysqlPacket.write(buffer);
            }
            rowsEof.write(buffer);
        }

    }

    @Override
    public int calcPacketSize() {
        return 0;
    }

    @Override
    public String getPacketInfo() {
        return null;
    }

    @Override
    public String toString() {
        return "ResultSetPacket{"
            + "\n, columnsNumber=" + columnsNumber
            + "\n, columns=" + columns
            + "\n, columnsEof=" + columnsEof
            + "\n, rows=" + rows
            + "\n, rowsEof=" + rowsEof
            + "\n, capabilities=" + capabilities
            + "\n}";
    }
}
