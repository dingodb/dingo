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

package io.dingodb.store;

import io.dingodb.common.util.FileUtils;
import io.dingodb.store.local.Configuration;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class SpilledMultiSortTest {
    protected static RocksDB db = null;

    private static final byte FIELD_SEPARATOR = 0x01;
    private static final byte ESCAPE_CHAR = 0x02;
    private static final byte NULL_PREFIX = 0x00;
    private static final byte NOT_NULL_PREFIX = 0x01;

    public static class SortField {
        public enum Type {
            STRING, INTEGER, LONG, DOUBLE, BOOLEAN, DATE
        }

        private Type type;
        private String fieldName;
        private boolean ascending;
        private boolean nullsFirst;

        public SortField(Type type, String fieldName, boolean ascending, boolean nullsFirst) {
            this.type = type;
            this.fieldName = fieldName;
            this.ascending = ascending;
            this.nullsFirst = nullsFirst;
        }
    }

    public static class Builder {
        private List<SortField> sortFields = new ArrayList<>();

        public Builder comparing(String fieldName) {
            return thenComparing(fieldName, SortField.Type.LONG, true, true);
        }

        public Builder thenComparing(String fieldName) {
            return thenComparing(fieldName, SortField.Type.STRING, true, true);
        }

        public Builder thenComparing(String fieldName, boolean ascending) {
            return thenComparing(fieldName, SortField.Type.STRING, ascending, true);
        }

        public Builder thenComparingInt(String fieldName) {
            return thenComparing(fieldName, SortField.Type.INTEGER, true, true);
        }

        public Builder thenComparingLong(String fieldName) {
            return thenComparing(fieldName, SortField.Type.LONG, true, true);
        }

        public Builder thenComparing(String fieldName, SortField.Type type,
                                     boolean ascending, boolean nullsFirst) {
            sortFields.add(new SortField(type, fieldName, ascending, nullsFirst));
            return this;
        }

        public ChainComparator build() {
            return new ChainComparator(sortFields);
        }
    }

    @lombok.Builder
    public static class ChainComparator {
        private List<SortField> sortFields;

        public ChainComparator(List<SortField> sortFields) {
            this.sortFields = sortFields;
        }

        // 编码实体为 RocksDB Key
        public byte[] encodeKey(Map<String, Object> entity, String uniqueId) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            baos.write("test".getBytes());
            for (SortField field : sortFields) {
                Object value = entity.get(field.fieldName);
                encodeField(baos, value, field);
                baos.write(FIELD_SEPARATOR);
            }

            // 添加唯一标识符，防止重复
            encodeString(baos, uniqueId);
            return baos.toByteArray();
        }

        private void encodeField(ByteArrayOutputStream baos, Object value, SortField field) {
            boolean isNull = (value == null);

            // NULL 值处理
            if (isNull) {
                baos.write(field.nullsFirst ? NULL_PREFIX : (byte)0xFF);
                return;
            }

            // 非 NULL 值
            baos.write(field.nullsFirst ? NOT_NULL_PREFIX : (byte)0x00);

            // 根据类型编码
            switch (field.type) {
                case STRING:
                    encodeString(baos, (String)value, field.ascending);
                    break;
                case INTEGER:
                    encodeInt(baos, (Integer)value, field.ascending);
                    break;
                case LONG:
                    encodeLong(baos, (Long)value, field.ascending);
                    break;
                case DOUBLE:
                    encodeDouble(baos, (Double)value, field.ascending);
                    break;
                case BOOLEAN:
                    encodeBoolean(baos, (Boolean)value, field.ascending);
                    break;
                default:
                    encodeString(baos, value.toString(), field.ascending);
            }
        }

        private void encodeString(ByteArrayOutputStream baos, String str) {
            encodeString(baos, str, true);
        }

        private void encodeString(ByteArrayOutputStream baos, String str, boolean ascending) {
            if (str == null) {
                baos.write(0);
                return;
            }

            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
            // 转义分隔符
            for (byte b : bytes) {
                if (b == FIELD_SEPARATOR || b == ESCAPE_CHAR) {
                    baos.write(ESCAPE_CHAR);
                }
                baos.write(ascending ? b : (byte)(~b & 0xFF));
            }
            baos.write(0);  // 字符串结束符
        }

        private void encodeInt(ByteArrayOutputStream baos, int value, boolean ascending) {
            // 处理整数排序：翻转符号位确保负数排在正数前面
            int encoded = value ^ (1 << 31);
            if (!ascending) {
                encoded = ~encoded;
            }
            for (int i = 3; i >= 0; i--) {
                baos.write((encoded >> (i * 8)) & 0xFF);
            }
        }

        private void encodeLong(ByteArrayOutputStream baos, long value, boolean ascending) {
            long encoded = value ^ (1L << 63);
            if (!ascending) {
                encoded = ~encoded;
            }
            for (int i = 7; i >= 0; i--) {
                baos.write((int)((encoded >> (i * 8)) & 0xFF));
            }
        }

        private void encodeDouble(ByteArrayOutputStream baos, double value, boolean ascending) {
            long bits = Double.doubleToLongBits(value);
            // 处理浮点数特殊排序
            if ((bits & (1L << 63)) == 0) {
                bits ^= 0xFFFFFFFFFFFFFFFFL;
            } else {
                bits ^= 0x8000000000000000L;
            }

            if (!ascending) {
                bits = ~bits;
            }

            for (int i = 7; i >= 0; i--) {
                baos.write((int)((bits >> (i * 8)) & 0xFF));
            }
        }

        private void encodeBoolean(ByteArrayOutputStream baos, boolean value, boolean ascending) {
            byte encoded = value ? (byte)1 : (byte)0;
            if (!ascending) {
                encoded = (byte)(1 - encoded);
            }
            baos.write(encoded);
        }
    }

    //@BeforeAll
    public static void init() throws RocksDBException {
        String path = "/root/tcpdump/cache";
        RocksDB rocksdb = null;
        try {
            Path dbPath = Paths.get(path);
            FileUtils.deleteIfExists(dbPath);
            FileUtils.createDirectories(dbPath);
            Options options = new Options();
            options.setCreateIfMissing(true);
            options.setWriteBufferSize(Configuration.instance().getBufferSize());
            options.setMaxWriteBufferNumber(Configuration.instance().getBufferNumber());
            options.setTargetFileSizeBase(Configuration.instance().getFileSize());
            rocksdb = RocksDB.open(options, path);
        } catch (Exception e) {
            throw e;
        }
        db = rocksdb;
    }

    //@Test
    public void multiSort() throws RocksDBException, IOException {
        ChainComparator comparator = new Builder()
            .thenComparing("score", SortField.Type.LONG, false, false)        // 第一排序字段
            .thenComparing("age", SortField.Type.LONG, true, false)      // 第三排序字段（整型）
            .thenComparing("gradle", SortField.Type.LONG, false, false) // 降序
            .build();

        put(getPerson1(), comparator);
        put(getPerson2(), comparator);
        put(getPerson3(), comparator);
        put(getPerson4(), comparator);
        put(getPerson5(), comparator);
        put(getPerson6(), comparator);
        System.out.println("init data");

        RocksIterator iterator = db.newIterator();

        iterator.seek("test".getBytes());

        while (iterator.isValid()) {
            Person person = Person.getPerson(iterator.value());
            System.out.println(person.toString());
            iterator.next();
        }
        System.out.println("end------------");
    }

    public void put(Person person, ChainComparator comparator) throws RocksDBException, IOException {
        // 转换为 Map
        Map<String, Object> data = new HashMap<>();
        data.put("gradle", person.gradle);
        data.put("age", person.age);
        data.put("score", person.getScore());
        // 生成排序 Key
        byte[] sortKey = comparator.encodeKey(data, person.getId());

        // 序列化实际数据
        byte[] value = Person.getBytes(person);

        // 写入 RocksDB
        db.put(sortKey, value);
    }

    public Person getPerson1() {
        return new Person("11", 30, 98, 3, "gjn");
    }

    public Person getPerson2() {
        return new Person("22", 25, 85, 3, "hu");
    }

    public Person getPerson3() {
        return new Person("33", 35, 87, 3, "niuh");
    }

    public Person getPerson4() {
        return new Person("44", 31, 90, 3, "song");
    }
    public Person getPerson5() {
        return new Person("45", 31, 87, 3, "song");
    }
    public Person getPerson6() {
        return new Person("46", 32, 87, 3, "song");
    }
}
