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

package org.apache.calcite.avatica.util;

import io.dingodb.driver.common.DingoArray;
import org.apache.calcite.avatica.ColumnMetaData.AvaticaType;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

public class DingoAccessor implements Cursor.Accessor {

    protected final AbstractCursor.Getter getter;

    protected Calendar calendar;

    public DingoAccessor(AbstractCursor.Getter getter) {
        this.getter = getter;
    }

    public DingoAccessor(AbstractCursor.Getter getter, Calendar calendar) {
        this.getter = getter;
        this.calendar = calendar;
    }

    public boolean wasNull() throws SQLException {
        return getter.wasNull();
    }

    public String getString() throws SQLException {
        final Object o = getObject();
        return o == null ? null : o.toString();
    }

    public boolean getBoolean() throws SQLException {
        return getLong() != 0L;
    }

    public byte getByte() throws SQLException {
        return (byte) getLong();
    }

    public short getShort() throws SQLException {
        return (short) getLong();
    }

    public int getInt() throws SQLException {
        return (int) getLong();
    }

    public long getLong() throws SQLException {
        throw new RuntimeException();
    }

    @Override
    public float getFloat() throws SQLException {
        Object o = getter.getObject();
        if (o instanceof Float) {
            return (float) o;
        }
        throw new RuntimeException();
    }

    @Override
    public double getDouble() throws SQLException {
        return 0;
    }

    @Override
    public BigDecimal getBigDecimal() throws SQLException {
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(int scale) throws SQLException {
        return null;
    }

    @Override
    public byte[] getBytes() throws SQLException {
        return new byte[0];
    }

    @Override
    public InputStream getAsciiStream() throws SQLException {
        return null;
    }

    @Override
    public InputStream getUnicodeStream() throws SQLException {
        return null;
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        return null;
    }

    @Override
    public Object getObject() throws SQLException {
        return getter.getObject();
    }

    @Override
    public Object getObject(Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public <T> T getObject(Class<T> type) throws SQLException {
        return null;
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        return null;
    }

    @Override
    public Ref getRef() throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob() throws SQLException {
        return null;
    }

    @Override
    public Clob getClob() throws SQLException {
        return null;
    }

    @Override
    public Array getArray() throws SQLException {
        return null;
    }

    @Override
    public Struct getStruct() throws SQLException {
        return null;
    }

    @Override
    public Date getDate(Calendar calendar) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(Calendar calendar) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(Calendar calendar) throws SQLException {
        return null;
    }

    @Override
    public URL getURL() throws SQLException {
        return null;
    }

    @Override
    public NClob getNClob() throws SQLException {
        return null;
    }

    @Override
    public SQLXML getSQLXML() throws SQLException {
        return null;
    }

    @Override
    public String getNString() throws SQLException {
        return null;
    }

    @Override
    public Reader getNCharacterStream() throws SQLException {
        return null;
    }

    public static class FloatAccessor extends DingoAccessor {
        public FloatAccessor(AbstractCursor cursor, int index) {
            super(cursor.createGetter(index));
        }

        public float getFloat() throws SQLException {
            Float o = (Float) getObject();
            return o == null ? 0f : o;
        }

        public double getDouble() throws SQLException {
            return getFloat();
        }
    }

    public static class ArrayAccessor extends DingoAccessor {
        private final AvaticaType component;

        public ArrayAccessor(AbstractCursor cursor, int index, Calendar calendar, AvaticaType component) {
            super(cursor.createGetter(index), calendar);
            this.component = component;
        }

        public ArrayAccessor(AbstractCursor cursor, int index, AvaticaType component) {
            super(cursor.createGetter(index), null);
            this.component = component;
        }

        @Override
        public Array getArray() throws SQLException {
            final Object o = getObject();
            if (o == null) {
                return null;
            }
            // If it's not an Array already, assume it is a List.
            List<Object> list = (List<Object>) o;
            if (!list.isEmpty()) {
                Object item = list.get(0);
                if (item instanceof Float) {
                    list = list.stream().map(e -> new BigDecimal(Float.toString((Float)e))).collect(Collectors.toList());
                }
            }

            TimeZone timeZone = TimeZone.getDefault();
            ArrayFactoryImpl factory = new ArrayFactoryImpl(timeZone);
            ResultSet resultSet = factory.create(component, list);
            return new DingoArray(list, calendar, resultSet);        }
    }

}
