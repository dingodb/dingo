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

package io.dingodb.calcite.stats;

import com.fasterxml.jackson.core.Base64Variant;
import com.fasterxml.jackson.core.Base64Variants;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlKind;

import java.nio.ByteBuffer;

public class CountMinSketch implements Cloneable, CalculateStatistic {

    @Getter
    @Setter
    private String schemaName;

    @Getter
    @Setter
    private String tableName;

    @Getter
    @Setter
    private String columnName;

    @Getter
    @Setter
    private long totalCount = 0;
    @Getter
    @Setter
    private long nullCount;

    @Getter
    @Setter
    private int index;

    private final int width;
    private final int depth;
    private final int[][] multiset;

    public CountMinSketch(String schemaName,
                          String tableName,
                          String columnName,
                          int index,
                          int width,
                          int depth) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.width = width;
        this.depth = depth;
        this.index = index;
        this.multiset = new int[depth][width];
    }

    private CountMinSketch(int width, int depth, int[][] ms) {
        this.width = width;
        this.depth = depth;
        this.multiset = ms;
    }

    public int getWidth() {
        return width;
    }

    public int getDepth() {
        return depth;
    }

    /**
     * Returns the size in bytes after serialization.
     *
     * @return serialized size in bytes
     */
    public long getSizeInBytes() {
        return ((width * depth) + 2) * (Integer.SIZE / 8);
    }

    public void set(byte[] key) {
        long hash64 = Murmur3.hash64(key);
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);
        for (int i = 1; i <= depth; i++) {
            int combinedHash = hash1 + (i * hash2);
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            int pos = combinedHash % width;
            multiset[i - 1][pos] += 1;
        }
    }

    public void setString(String val) {
        totalCount ++;
        if (val == null) {
            nullCount ++;
            return;
        }
        set(val.getBytes());
    }

    public void setByte(byte val) {
        totalCount ++;
        set(new byte[]{val});
    }

    public void setInt(Integer val) {
        totalCount ++;
        if (val == null) {
            nullCount++;
            return;
        }
        set(intToByteArrayLE(val));
    }


    public void setLong(Long val) {
        totalCount ++;
        if (val == null) {
            nullCount++;
            return;
        }
        set(longToByteArrayLE(val));
    }

    public void setFloat(Float val) {
        totalCount ++;
        if (val == null) {
            nullCount++;
            return;
        }
        setInt(Float.floatToIntBits(val));
    }

    public void setDouble(Double val) {
        totalCount ++;
        if (val == null) {
            nullCount++;
            return;
        }
        setLong(Double.doubleToLongBits(val));
    }

    private static byte[] intToByteArrayLE(int val) {
        return new byte[]{(byte) (val >> 0),
            (byte) (val >> 8),
            (byte) (val >> 16),
            (byte) (val >> 24)};
    }

    private static byte[] longToByteArrayLE(long val) {
        return new byte[]{(byte) (val >> 0),
            (byte) (val >> 8),
            (byte) (val >> 16),
            (byte) (val >> 24),
            (byte) (val >> 32),
            (byte) (val >> 40),
            (byte) (val >> 48),
            (byte) (val >> 56),};
    }

    public int getEstimatedCount(byte[] key) {
        long hash64 = Murmur3.hash64(key);
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);
        int min = Integer.MAX_VALUE;
        for (int i = 1; i <= depth; i++) {
            int combinedHash = hash1 + (i * hash2);
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            int pos = combinedHash % width;
            min = Math.min(min, multiset[i - 1][pos]);
        }

        return min;
    }

    public int getEstimatedCountString(String val) {
        return getEstimatedCount(val.getBytes());
    }

    public int getEstimatedCountByte(byte val) {
        return getEstimatedCount(new byte[]{val});
    }

    public int getEstimatedCountInt(int val) {
        return getEstimatedCount(intToByteArrayLE(val));
    }

    public int getEstimatedCountLong(long val) {
        return getEstimatedCount(longToByteArrayLE(val));
    }

    public int getEstimatedCountFloat(float val) {
        return getEstimatedCountInt(Float.floatToIntBits(val));
    }

    public int getEstimatedCountDouble(double val) {
        return getEstimatedCountLong(Double.doubleToLongBits(val));
    }

    public double estimateSelectivity(SqlKind op, Object valObj) {
        switch (op) {
            case EQUALS:
                return getSelectivityEquals(valObj);
            case NOT_EQUALS:
                return 1 - getSelectivityEquals(valObj);
            case IS_NULL:
                return getSelectivityIsNull();
            case IS_NOT_NULL:
                return 1 - getSelectivityIsNull();
            default:
                return 0.25;
        }
    }

    private double getSelectivityEquals(Object val) {
        String valStr = val.toString();
        double estimatedCount = getEstimatedCountString(valStr);
        return estimatedCount / totalCount;
    }

    private double getSelectivityIsNull() {
        return nullCount / (double) totalCount;
    }

    public void merge(CountMinSketch that) {
        if (that == null) {
            return;
        }

        if (this.width != that.width) {
            throw new RuntimeException("Merge failed! Width of count min sketch do not match!"
                + "this.width: " + this.getWidth() + " that.width: " + that.getWidth());
        }

        if (this.depth != that.depth) {
            throw new RuntimeException("Merge failed! Depth of count min sketch do not match!"
                + "this.depth: " + this.getDepth() + " that.depth: " + that.getDepth());
        }
        this.nullCount += that.nullCount;
        for (int i = 0; i < depth; i++) {
            for (int j = 0; j < width; j++) {
                this.multiset[i][j] += that.multiset[i][j];
            }
        }
    }

    public String serialize() {
        long serializedSize = this.getSizeInBytes();
        ByteBuffer bb = ByteBuffer.allocate((int) serializedSize);
        bb.putInt(this.getWidth());
        bb.putInt(this.getDepth());
        for (int i = 0; i < this.getDepth(); i++) {
            for (int j = 0; j < this.getWidth(); j++) {
                bb.putInt(this.multiset[i][j]);
            }
        }
        bb.flip();
        byte[] tmp = bb.array();
        Base64Variant base64Variant = Base64Variants.getDefaultVariant();
        return base64Variant.encode(tmp);
    }

    public static CountMinSketch deserialize(String cmSketchStr) {
        byte[] serialized = Base64Variants.getDefaultVariant().decode(cmSketchStr);
        ByteBuffer bb = ByteBuffer.allocate(serialized.length);
        bb.put(serialized);
        bb.flip();
        int width = bb.getInt();
        int depth = bb.getInt();
        int[][] multiset = new int[depth][width];
        for (int i = 0; i < depth; i++) {
            for (int j = 0; j < width; j++) {
                multiset[i][j] = bb.getInt();
            }
        }
        return new CountMinSketch(width, depth, multiset);
    }

    public CountMinSketch copy() {
        return new CountMinSketch(schemaName, tableName, columnName, index, width, depth);
    }
}
