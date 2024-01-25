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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dingodb.common.type.DingoType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.calcite.sql.SqlKind;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;

@NoArgsConstructor
@JsonPropertyOrder({"max", "min", "width", "lstWidth", "buckets", "totalCount"})
public class Histogram implements Cloneable, CalculateStatistic {

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
    @JsonIgnore
    private DingoType dingoType;

    @Getter
    @Setter
    private int index;

    /**
     * max value in the histogram.
     */
    @JsonProperty("max")
    private Long max;

    /**
     * min value in the histogram.
     */
    @JsonProperty("min")
    private Long min;

    /**
     * width of each bucket, except for the last.
     */
    @JsonProperty("width")
    private long width;

    /**
     * width of the last bucket.
     */
    @JsonProperty("lstWidth")
    private long lstWidth;

    /**
     * count of each bucket.
     */
    @JsonProperty("buckets")
    private long[] buckets;

    /**
     * sum of counts of all baskets.
     */
    @JsonProperty("totalCount")
    @Getter
    private long totalCount;

    @JsonProperty("nullCount")
    private long nullCount;

    public Histogram(String schemaName,
                        String tableName,
                        String columnName,
                        DingoType dingoType,
                        int index) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.dingoType = dingoType;
        this.index = index;
    }

    public void setRegionMax(Object regionMax) {
        Long val = getLongVal(regionMax);
        if (max == null) {
            max = val;
        } else if (val > max) {
            max = val;
        }
    }

    public void setRegionMin(Object regionMin) {
        Long val = getLongVal(regionMin);
        if (min == null) {
            min = val;
        } else if (val < min) {
            min = val;
        }
    }

    public void init(int buckets) {
        if (this.max == null || this.min == null) {
            return;
        }
        // calculate # of buckets
        if (this.max - this.min + 1 < buckets) {
            this.buckets = new long[(int) (this.max - this.min + 1)];
            width = 1;
            lstWidth = 1;
        } else {
            this.buckets = new long[buckets];
            width = (int) Math.floor((this.max - this.min + 1) / (double) buckets);
            lstWidth = (this.max - (min + (buckets - 1) * width)) + 1;
        }

        // set everything to 0, for safety
        Arrays.fill(this.buckets, 0);

        // set totalCount to 0
        totalCount = 0;
    }

    public void addValue(Object val) {
        totalCount++;
        if (val == null) {
            nullCount ++;
            return;
        }
        Long value = getLongVal(val);
        addLongValue(value);
    }

    private static Long getLongVal(Object val) {
        long value = 0;
        if (val instanceof Integer) {
            value = ((Integer) val).longValue();
        } else if (val instanceof BigDecimal) {
            value = ((BigDecimal) val).longValue();
        } else if (val instanceof Date) {
            value = ((Date) val).getTime();
        } else if (val instanceof Time) {
            value = ((Time) val).getTime();
        } else if (val instanceof Timestamp) {
            value = ((Timestamp) val).getTime();
        } else if (val instanceof Long) {
            value = (long) val;
        } else if (val instanceof Double) {
            value = ((Double) val).longValue();
        } else if (val instanceof Float) {
            value = ((Float) val).longValue();
        }
        return value;
    }

    public void merge(Histogram histogram) {
        this.totalCount += histogram.totalCount;
        this.nullCount += histogram.nullCount;
        for (int i = 0; i < buckets.length; i ++) {
            buckets[i] += histogram.buckets[i];
        }
    }

    private void addLongValue(Long val) {
        if (min == null || max == null) {
            return;
        }
        if (val < min || val > max) {
            return;
        }

        if ((val - min) / width < buckets.length - 1) {
            buckets[(int) ((val - min) / width)]++;
        } else {
            buckets[buckets.length - 1]++;
        }

    }

    public double estimateSelectivity(SqlKind op, Object valObj) {
        Long val = getLongVal(valObj);
        switch (op) {
            case EQUALS:
            case LIKE:
                return estimateSelectivityEquals(val);
            case NOT_EQUALS:
                return 1 - estimateSelectivityEquals(val);
            case GREATER_THAN:
                return estimateSelectivityGreaterThan(val);
            case LESS_THAN_OR_EQUAL:
                return estimateSelectivityLessThan(val) + estimateSelectivityEquals(val);
            case LESS_THAN:
                return estimateSelectivityLessThan(val);
            case GREATER_THAN_OR_EQUAL:
                return estimateSelectivityGreaterThan(val) + estimateSelectivityEquals(val);
            case IS_NOT_NULL:
                return 1 - estimateSelectivityIsNull();
            case IS_NULL:
                return estimateSelectivityIsNull();
            default:
                return 0.25;
        }
    }

    public Histogram copy() {
        Histogram histogram = new Histogram(schemaName, tableName, columnName, dingoType, index);
        if (max == null || min == null || buckets == null) {
            return histogram;
        }
        histogram.setRegionMin(min);
        histogram.setRegionMax(max);
        histogram.init(buckets.length);
        return histogram;
    }

    public String serialize() {
        ObjectMapper objectMapper = new ObjectMapper();
        String histogramDetail;
        try {
            histogramDetail = objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return histogramDetail;
    }

    public static Histogram deserialize(String str) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(str, Histogram.class);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    private double estimateSelectivityEquals(long val) {
        if (val < min || val > max) {
            return 0;
        }

        int b = (int) Math.min((val - min) / width, buckets.length - 1);

        if (b < buckets.length - 1) {
            return (buckets[b] / (double) width) / totalCount;
        } else {
            return (buckets[b] / (double) lstWidth) / totalCount;
        }
    }

    private double estimateSelectivityGreaterThan(long val) {
        if (val < min) {
            return 1;
        } else if (val >= max) {
            return 0;
        }

        int b = (int) Math.min((val - min) / width, buckets.length - 1);

        double bf = buckets[b] / (double) totalCount;
        if (b < buckets.length - 1) {
            double rv = ((min + (b + 1) * width - 1 - val) / (double) width) * bf;
            for (int i = b + 1; i < buckets.length; i++) {
                rv += buckets[i] / (double) totalCount;
            }
            return rv;
        } else {
            return ((max - val) / (double) lstWidth) * bf;
        }
    }

    private double estimateSelectivityLessThan(long val) {
        if (val <= min) {
            return 0;
        } else if (val > max) {
            return 1;
        }

        int b = (int) Math.min((val - min) / width, buckets.length - 1);

        double bf = buckets[b] / (double) totalCount;
        if (b < buckets.length - 1) {
            double rv = (((min + (b + 1) * width) - val - 1) / (double) width) * bf;
            for (int i = b - 1; i >= 0; i--) {
                rv += buckets[i] / (double) totalCount;
            }
            return rv;
        } else {
            double rv = ((max - val) / (double) lstWidth) * bf;
            for (int i = b - 1; i >= 0; i--) {
                rv += buckets[i] / (double) totalCount;
            }
            return rv;
        }
    }

    public double estimateSelectivityIsNull() {
        return nullCount / (double)totalCount;
    }

}
