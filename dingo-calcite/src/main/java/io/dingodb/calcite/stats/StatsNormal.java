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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.scalar.DateType;
import io.dingodb.common.type.scalar.DoubleType;
import io.dingodb.common.type.scalar.FloatType;
import io.dingodb.common.type.scalar.IntegerType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.type.scalar.StringType;
import io.dingodb.common.type.scalar.TimeType;
import io.dingodb.common.type.scalar.TimestampType;
import lombok.extern.slf4j.Slf4j;
import net.agkn.hll.HLL;

import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashSet;

@Slf4j
public class StatsNormal implements Cloneable {
    private final String columnName;
    private Long ndv;
    private Long numNull = 0L;

    private Long totalCount = 0L;

    private DingoType type;
    private long totalColSize = 0L;
    private long avgColSize;
    HashSet hashSet = null;
    HashFunction hash = null;
    HLL hll = null;

    public StatsNormal(String columnName,
                       DingoType dingoType) {
        this.columnName = columnName;
        this.type = dingoType;
        hash = Hashing.murmur3_128(12345678);
        hll = new HLL(13, 5);
    }

    public StatsNormal(String columnName, Long ndv, Long numNull, long avgColSize, long totalCount) {
        this.columnName = columnName;
        this.ndv = ndv;
        this.numNull = numNull;
        this.avgColSize = avgColSize;
        this.totalCount = totalCount;
    }

    public void addStringVal(String val) {
        hll.addRaw(hash.newHasher().putString(val, Charset.defaultCharset()).hash().asLong());
    }

    public void addIntVal(Integer val) {
        hll.addRaw(hash.newHasher().putInt(val).hash().asLong());
    }

    public void addLongVal(Long val) {
        hll.addRaw(hash.newHasher().putLong(val).hash().asLong());
    }

    public void addDoubleVal(Double val) {
        hll.addRaw(hash.newHasher().putDouble(val).hash().asLong());
    }

    public void addFloatVal(Float val) {
        hll.addRaw(hash.newHasher().putFloat(val).hash().asLong());
    }

    public void addCharVal(char val) {
        hll.addRaw(hash.newHasher().putChar(val).hash().asLong());
    }

    public void addDateVal(Date val) {
        hll.addRaw(hash.newHasher().putLong(val.getTime()).hash().asLong());
    }

    public void addTimeVal(Time val) {
        hll.addRaw(hash.newHasher().putLong(val.getTime()).hash().asLong());
    }

    public void addTimestampVal(Timestamp val) {
        hll.addRaw(hash.newHasher().putLong(val.getTime()).hash().asLong());
    }

    public void setNdv() {
        if (hashSet != null) {
            this.ndv = (long) hashSet.size();
        } else {
            this.ndv = hll.cardinality();
        }
    }

    public StatsNormal merge(StatsNormal statsNormal) {
        this.ndv += statsNormal.getNdv();
        this.numNull += statsNormal.numNull;
        this.totalColSize += statsNormal.totalColSize;
        return this;
    }

    public void addVal(Object val) {
        totalCount ++;
        if (val == null) {
            numNull ++;
            return;
        }
        if (type instanceof StringType) {
            totalColSize += ((String) val).length() * 2L;
        }
        if (hashSet != null) {
            hashSet.add(val);
            return;
        }
        if (type instanceof DoubleType) {
            addDoubleVal((Double)val);
        } else if (type instanceof IntegerType) {
            addIntVal((Integer) val);
        } else if (type instanceof StringType) {
            addStringVal((String) val);
        } else if (type instanceof LongType) {
            addLongVal((Long) val);
        } else if (type instanceof DateType) {
            addDateVal((Date) val);
        } else if (type instanceof TimeType) {
            addTimeVal((Time) val);
        } else if (type instanceof TimestampType) {
            addTimestampVal((Timestamp) val);
        } else if (type instanceof FloatType) {
            addFloatVal((Float) val);
        }
    }

    public void clear() {
        if (hashSet != null) {
            hashSet.clear();
        }
        if (hll != null) {
            hll.clear();
        }
    }

    public void calculateAvgColSize() {
        if (totalCount == 0) {
            avgColSize = 0;
            return;
        }
        int baseSize = 0;
        if (type instanceof IntegerType
            || type instanceof FloatType
            || type instanceof TimeType
            || type instanceof TimestampType
            || type instanceof DateType
        ) {
            baseSize = 4;
        } else if (type instanceof DoubleType || type instanceof LongType) {
            baseSize = 8;
        }
        if (baseSize > 0 && totalCount > 0) {
            avgColSize = baseSize * (1 - (numNull / totalCount));
        } else {
            long nonNumNull = totalCount - numNull;
            if (nonNumNull > 0) {
                avgColSize = totalColSize / nonNumNull;
            }
        }
    }

    public StatsNormal copy() {
        return new StatsNormal(columnName, type);
    }

    public String getColumnName() {
        return columnName;
    }

    public Long getNdv() {
        return ndv;
    }

    public Long getNumNull() {
        return numNull;
    }

    public long getAvgColSize() {
        return avgColSize;
    }

    public Long getTotalCount() {
        return totalCount;
    }
}
