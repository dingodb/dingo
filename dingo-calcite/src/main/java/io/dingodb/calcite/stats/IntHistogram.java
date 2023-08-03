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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dingodb.common.type.DingoType;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;

@Slf4j
@JsonPropertyOrder({"max", "min", "width", "lstWidth", "buckets", "numTuples"})
public class IntHistogram implements Cloneable, CalculateStatistic {
    private final String schemaName;

    private final String tableName;

    private final String columnName;

    private final DingoType dingoType;

    private final int index;

    /**
     * max value in the histogram.
     */
    @JsonProperty("max")
    private int max;

    /**
     * min value in the histogram.
     */
    @JsonProperty("min")
    private int min;

    /**
     * width of each bucket, except for the last.
     */
    @JsonProperty("width")
    private int width;

    /**
     * width of the last bucket.
     */
    @JsonProperty("lstWidth")
    private int lstWidth;

    /**
     * count of each bucket.
     */
    @JsonProperty("buckets")
    private int[] buckets;

    /**
     * sum of counts of all baskets.
     */
    @JsonProperty("numTuples")
    private int numTuples;

    public IntHistogram(String schemaName,
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
        Integer val = getInteger(regionMax);
        if (val > max) {
            max = val;
        }
    }

    public void setRegionMin(Object regionMin) {
        Integer val = getInteger(regionMin);
        if (val < min) {
            min = val;
        }
    }

    public void init(int buckets) {
        // calculate # of buckets
        if (this.max - this.min + 1 < buckets) {
            this.buckets = new int[this.max - this.min + 1];
            width = 1;
            lstWidth = 1;
        } else {
            this.buckets = new int[buckets];
            width = (int) Math.floor((this.max - this.min + 1) / (double) buckets);
            lstWidth = (this.max - (min + (buckets - 1) * width)) + 1;
        }

        // set everything to 0, for safety
        Arrays.fill(this.buckets, 0);

        // set numTuples to 0
        numTuples = 0;
    }

    public void addValue(Object val) {
        if (val == null) {
            addIntValue(null);
            return;
        }
        Integer value = getInteger(val);
        addIntValue(value);
    }

    private static Integer getInteger(Object val) {
        int value = 0;
        if (val instanceof Integer) {
            value = (int) val;
        } else if (val instanceof BigDecimal) {
            value = ((BigDecimal) val).intValue();
        } else if (val instanceof Date) {
            value = ((Long) ((Date) val).getTime()).intValue();
        } else if (val instanceof Time) {
            value = ((Long) ((Time) val).getTime()).intValue();
        } else if (val instanceof Timestamp) {
            value = ((Long) ((Timestamp) val).getTime()).intValue();
        } else if (val instanceof Long) {
            value = ((Long) val).intValue();
        } else if (val instanceof Double) {
            value = ((Double) val).intValue();
        } else if (val instanceof Float) {
            value = ((Float) val).intValue();
        }
        return value;
    }

    public void merge(IntHistogram intHistogram) {
        this.numTuples += intHistogram.numTuples;
        for (int i = 0; i < buckets.length; i ++) {
            buckets[i] += intHistogram.buckets[i];
        }
    }

    private void addIntValue(Integer val) {
        numTuples++;
        if (val == null) {
            return;
        }
        if (val < min || val > max) {
            log.error("range out");
        }

        if ((val - min) / width < buckets.length - 1) {
            buckets[(val - min) / width]++;
        } else {
            buckets[buckets.length - 1]++;
        }

    }

    public double estimateSelectivity(SqlKind op, Object valObj) {
        Integer val = getInteger(valObj);
        switch (op) {
            case EQUALS:
                if (val < min || val > max) {
                    return 0;
                }

                int b = Math.min((val - min) / width, buckets.length - 1);

                if (b < buckets.length - 1) {
                    return (buckets[b] / (double) width) / numTuples;
                } else {
                    return (buckets[b] / (double) lstWidth) / numTuples;
                }
            case LIKE:
                if (val < min || val > max) {
                    return 0;
                }

                b = Math.min((val - min) / width, buckets.length - 1);

                if (b < buckets.length - 1) {
                    return (buckets[b] / (double) width) / numTuples;
                } else {
                    return (buckets[b] / (double) lstWidth) / numTuples;
                }
            case NOT_EQUALS:
                if (val < min || val > max) {
                    return 1;
                }

                b = Math.min((val - min) / width, buckets.length - 1);

                if (b < buckets.length - 1) {
                    return 1 - (buckets[b] / (double) width) / numTuples;
                } else {
                    return 1 - (buckets[b] / (double) lstWidth) / numTuples;
                }
            case GREATER_THAN:
                if (val < min) {
                    return 1;
                } else if (val >= max) {
                    return 0;
                }

                b = Math.min((val - min) / width, buckets.length - 1);

                double bf = buckets[b] / (double) numTuples;
                if (b < buckets.length - 1) {
                    double rv = ((min + (b + 1) * width - 1 - val) / (double) width) * bf;
                    for (int i = b + 1; i < buckets.length; i++) {
                        rv += buckets[i] / (double) numTuples;
                    }
                    return rv;
                } else {
                    return ((max - val) / (double) lstWidth) * bf;
                }
            case LESS_THAN_OR_EQUAL:
                if (val < min) {
                    return 0;
                } else if (val >= max) {
                    return 1;
                }

                b = Math.min((val - min) / width, buckets.length - 1);

                bf = buckets[b] / (double) numTuples;
                if (b < buckets.length - 1) {
                    bf = buckets[b] / (double) numTuples;
                    double rv = ((min + (b + 1) * width - 1 - val) / (double) width) * bf;
                    for (int i = b + 1; i < buckets.length; i++) {
                        rv += buckets[i] / (double) numTuples;
                    }
                    return 1 - rv;
                } else {
                    return 1 - ((max - val) / (double) lstWidth) * bf;
                }
            case LESS_THAN:
                if (val <= min) {
                    return 0;
                } else if (val > max) {
                    return 1;
                }

                b = Math.min((val - min) / width, buckets.length - 1);

                bf = buckets[b] / (double) numTuples;
                if (b < buckets.length - 1) {
                    double rv = (((min + (b + 1) * width) - val - 1) / (double) width) * bf;
                    for (int i = b - 1; i >= 0; i--) {
                        rv += buckets[i] / (double) numTuples;
                    }
                    return rv;
                } else {
                    double rv = ((max - val) / (double) lstWidth) * bf;
                    for (int i = b - 1; i >= 0; i--) {
                        rv += buckets[i] / (double) numTuples;
                    }
                    return rv;
                }
            case GREATER_THAN_OR_EQUAL:
                if (val <= min) {
                    return 1;
                } else if (val > max) {
                    return 0;
                }

                b = Math.min((val - min) / width, buckets.length - 1);

                bf = buckets[b] / (double) numTuples;
                if (b < buckets.length - 1) {
                    double rv = (((min + (b + 1) * width) - val) / (double) width) * bf;
                    for (int i = b + 1; i < buckets.length; i++) {
                        rv += buckets[i] / (double) numTuples;
                    }
                    return rv;
                } else {
                    return ((max - val + 1) / (double) lstWidth) * bf;
                }
            default:
                throw new IllegalArgumentException("op not supported");
        }
    }

    @Override
    protected IntHistogram clone() {
        IntHistogram o = null;
        try {
            o = (IntHistogram) super.clone();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return o;
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

    @Override
    public IntHistogram deserialize(String str) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(str, IntHistogram.class);
        } catch (JsonProcessingException e) {
            return null;
        }
    }


    public String toString() {
        StringBuilder rv = new StringBuilder("All buckets are right exclusive except for the last one:\r\n");
        for (int i = 0; i < buckets.length; i++) {
            if (i < buckets.length - 1) {
                rv.append("  ").append(min + i * (max - min)).append("-").append(min + (i + 1) * (max - min)).append(": ").append(buckets[i]).append("\r\n");
            } else {
                rv.append("  ").append(min + i * (max - min)).append("-").append(max).append(": ").append(buckets[i]);
            }
        }

        return rv.toString();
    }

    public String getColumnName() {
        return columnName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public DingoType getDingoType() {
        return dingoType;
    }

    public int getIndex() {
        return index;
    }
}
