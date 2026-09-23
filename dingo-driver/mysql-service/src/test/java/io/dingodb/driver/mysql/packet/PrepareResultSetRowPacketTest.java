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

import io.dingodb.common.mysql.constant.ColumnStatus;
import io.dingodb.driver.mysql.MysqlType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import javax.sql.rowset.RowSetMetaDataImpl;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class PrepareResultSetRowPacketTest {
    @Test
    void computedBooleanUsesAdvertisedBigintWidthWithoutShiftingNextColumn() throws SQLException {
        RowSetMetaDataImpl metaData = new RowSetMetaDataImpl();
        metaData.setColumnCount(3);
        metaData.setColumnTypeName(1, "BOOLEAN");
        metaData.setColumnType(1, Types.BOOLEAN);
        metaData.setColumnName(1, "comparison");
        metaData.setColumnLabel(1, "comparison");
        metaData.setTableName(1, "");
        metaData.setNullable(1, ResultSetMetaData.columnNullable);
        metaData.setColumnTypeName(2, "BOOLEAN");
        metaData.setColumnType(2, Types.BOOLEAN);
        metaData.setColumnName(2, "declared_boolean");
        metaData.setColumnLabel(2, "declared_boolean");
        metaData.setTableName(2, "sample");
        metaData.setNullable(2, ResultSetMetaData.columnNoNulls);
        metaData.setColumnTypeName(3, "INTEGER");
        metaData.setColumnType(3, Types.INTEGER);
        metaData.setColumnName(3, "following");
        metaData.setColumnLabel(3, "following");
        metaData.setTableName(3, "");

        List<ColumnPacket> columns = new ArrayList<>();
        MysqlPacketFactory.getInstance().addColumnPacketFromMeta(
            new AtomicLong(1), metaData, columns, "def", "UTF-8"
        );
        assertThat(columns.get(0).type).isEqualTo((byte) MysqlType.FIELD_TYPE_LONGLONG);
        assertThat(columns.get(1).type).isEqualTo((byte) MysqlType.FIELD_TYPE_TINY);
        assertThat(columns.get(0).flags & ColumnStatus.COLUMN_NOT_NULL).isZero();
        assertThat(columns.get(1).flags & ColumnStatus.COLUMN_NOT_NULL)
            .isEqualTo(ColumnStatus.COLUMN_NOT_NULL);

        PrepareResultSetRowPacket packet = new PrepareResultSetRowPacket();
        packet.setMetaData(metaData);
        packet.values.add(Boolean.TRUE);
        packet.values.add(Boolean.FALSE);
        packet.values.add(7);
        ByteBuf bytes = Unpooled.buffer();
        try {
            packet.write(bytes);
            assertThat(bytes.readUnsignedMediumLE()).isEqualTo(15);
            assertThat(bytes.readUnsignedByte()).isZero(); // packet sequence ID
            assertThat(bytes.readUnsignedByte()).isZero(); // binary row marker
            assertThat(bytes.readUnsignedByte()).isZero(); // null bitmap
            assertThat(bytes.readLongLE()).isEqualTo(1L);
            assertThat(bytes.readUnsignedByte()).isZero(); // declared TINYINT(1)
            assertThat(bytes.readIntLE()).isEqualTo(7);
            assertThat(bytes.isReadable()).isFalse();
        } finally {
            bytes.release();
        }
    }

    @Test
    void preparedTextRowUsesNegotiatedUtf8Encoding() throws SQLException {
        RowSetMetaDataImpl metaData = new RowSetMetaDataImpl();
        metaData.setColumnCount(1);
        metaData.setColumnTypeName(1, "VARCHAR");
        metaData.setColumnType(1, Types.VARCHAR);

        PrepareResultSetRowPacket packet = new PrepareResultSetRowPacket();
        packet.setMetaData(metaData);
        packet.setColumnEncoders(MysqlPacketFactory.getColumnEncoders(metaData, "UTF-8"));
        packet.addColumnValue("€", null);

        ByteBuf bytes = Unpooled.buffer();
        try {
            packet.write(bytes);
            assertThat(bytes.readUnsignedMediumLE()).isEqualTo(6);
            bytes.readUnsignedByte(); // packet sequence ID
            bytes.readUnsignedByte(); // binary row marker
            bytes.readUnsignedByte(); // null bitmap
            assertThat(bytes.readUnsignedByte()).isEqualTo((short) 3);
            assertThat(bytes.readUnsignedByte()).isEqualTo((short) 0xE2);
            assertThat(bytes.readUnsignedByte()).isEqualTo((short) 0x82);
            assertThat(bytes.readUnsignedByte()).isEqualTo((short) 0xAC);
            assertThat(bytes.isReadable()).isFalse();
        } finally {
            bytes.release();
        }
    }
}
