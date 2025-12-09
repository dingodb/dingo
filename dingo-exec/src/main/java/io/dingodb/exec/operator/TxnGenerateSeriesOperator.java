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

package io.dingodb.exec.operator;

import io.dingodb.codec.CodecService;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.profile.OperatorProfile;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.converter.DingoConverter;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.TxnGenerateSeriesParam;
import io.dingodb.expr.common.type.IntervalDayType;
import io.dingodb.expr.common.type.IntervalMonthType;
import io.dingodb.expr.common.type.IntervalType;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.StoreInstance;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TxnGenerateSeriesOperator extends FilterProjectSourceOperator {

    public static final TxnGenerateSeriesOperator INSTANCE = new TxnGenerateSeriesOperator();

    @Override
    protected @NonNull Iterator<Object[]> createSourceIterator(Vertex vertex) {
        TxnGenerateSeriesParam param = vertex.getParam();
        OperatorProfile profile = param.getProfile("generateSeries");
        long start = System.currentTimeMillis();
        Table table = param.getTable();
        Object startCol = param.getStartCol();
        Object endCol = param.getEndCol();
        int startIndex = table.getColumnIndex((Column) startCol);
        int endIndex = table.getColumnIndex((Column) endCol);
        Column endColumn = (Column) endCol;

        StoreInstance instance = Services.KV_STORE.getInstance(param.getTableId(), param.getPartId());
        RangeDistribution distribution = param.getRangeDistribution();
        CommonId partId = distribution.getId();
        byte[] startKey = distribution.getStartKey();
        byte[] endKey = distribution.getEndKey();
        CodecService.getDefault().setId(startKey, partId.domain);
        CodecService.getDefault().setId(endKey, partId.domain);
        StoreInstance.Range range = new StoreInstance.Range(startKey, endKey, true, true);
        Iterator<KeyValue> scan = instance.txnScan(param.getScanTs(), range, param.getTimeout());

        List<Object[]> intervals = new ArrayList<>();
        while (scan.hasNext()) {
            KeyValue keyValue = scan.next();
            Object[] tuple = param.getCodec().decode(keyValue);
            Object[] keys = new Object[param.getMapping() == null ? 0 : param.getMapping().size()];
            if (param.getMapping() != null) {
                keys = param.getMapping().revMap(tuple);
            }

            Object startObj = tuple[startIndex];
            Object endObj = tuple[endIndex];
            if (startObj instanceof Integer && endObj instanceof Integer) {
                List<Integer> list = generateIntegerIntervals(
                    (Integer) startObj,
                    (Integer) endObj,
                    param.getBigDecimalStep().intValue());
                intervals.addAll(merge(keys, Arrays.asList(list.toArray())));
            } else if (startObj instanceof Long && endObj instanceof Long) {
                List<Long> list = generateLongIntervals(
                    (Long) startObj,
                    (Long) endObj,
                    param.getBigDecimalStep().longValue());
                intervals.addAll(merge(keys, Arrays.asList(list.toArray())));
            } else if (startObj instanceof Double && endObj instanceof Double) {
                List<Double> list = generateDoubleIntervals(
                    (Double) startObj,
                    (Double) endObj,
                    param.getBigDecimalStep().doubleValue());
                intervals.addAll(merge(keys, Arrays.asList(list.toArray())));
            } else if (startObj instanceof Date && endObj instanceof Date) {
                List<Date> list = generateDateIntervals(
                    ((Date) startObj).toLocalDate(),
                    ((Date) endObj).toLocalDate(),
                    getPeriod(param.getIntervalStep()));
                intervals.addAll(merge(keys, Arrays.asList(list.toArray())));
            } else if (startObj instanceof Number && endObj instanceof Number) {
                List<BigDecimal> list = generateNumericIntervals(
                    new BigDecimal(startObj.toString()),
                    new BigDecimal(endObj.toString()),
                    param.getBigDecimalStep(),
                    BigDecimal::add,
                    endColumn.getType()
                );
                intervals.addAll(merge(keys, Arrays.asList(list.toArray())));
            } else if (startObj instanceof Timestamp && endObj instanceof Timestamp) {
                List<Timestamp> list = generateTimestampIntervals(
                    ((Timestamp) startObj).toLocalDateTime(),
                    ((Timestamp) endObj).toLocalDateTime(),
                    getPeriod(param.getIntervalStep())
                );
                intervals.addAll(merge(keys, Arrays.asList(list.toArray())));
            }
        }

        profile.incrTime(start);
        return intervals.iterator();
    }

    public static List<Integer> generateIntegerIntervals(int start, int end, int interval) {
        validateNumericParameters(start, end, interval);

        List<Integer> intervals = new ArrayList<>();
        int current = start;

        if (interval > 0) {
            while (current <= end) {
                intervals.add(current);
                current += interval;
            }
        } else {
            while (current >= end) {
                intervals.add(current);
                current += interval;
            }
        }

        return intervals;
    }

    public static List<Long> generateLongIntervals(long start, long end, long interval) {
        validateNumericParameters(start, end, interval);

        List<Long> intervals = new ArrayList<>();
        long current = start;

        if (interval > 0) {
            while (current <= end) {
                intervals.add(current);
                current += interval;
            }
        } else {
            while (current >= end) {
                intervals.add(current);
                current += interval;
            }
        }

        return intervals;
    }

    public static List<Double> generateDoubleIntervals(double start, double end, double interval) {
        if (interval == 0) {
            throw new IllegalArgumentException("interval cannot be 0");
        }
        if ((interval > 0 && start > end) || (interval < 0 && start < end)) {
            throw new IllegalArgumentException("Start and end value ranges do not match interval direction");
        }

        List<Double> intervals = new ArrayList<>();
        double current = start;

        // Handling floating point precision issues
        if (interval > 0) {
            while (current <= end + 1e-10) {
                intervals.add(current);
                current += interval;
            }
        } else {
            while (current >= end - 1e-10) {
                intervals.add(current);
                current += interval;
            }
        }

        return intervals;
    }

    public static <T extends Number & Comparable<T>> List<T> generateNumericIntervals(
        T start, T end, T interval, java.util.function.BinaryOperator<T> adder, DingoType type) {

        if (start == null || end == null || interval == null || adder == null) {
            throw new IllegalArgumentException("Parameter cannot be null");
        }

        List<T> intervals = new ArrayList<>();
        T current = start;

        int comparison = start.compareTo(end);
        int intervalSign = interval.compareTo((T) BigDecimal.valueOf(0));

        if ((comparison <= 0 && intervalSign <= 0) || (comparison >= 0 && intervalSign >= 0)) {
            throw new IllegalArgumentException("Start value, end value and direction of interval do not match");
        }

        if (comparison <= 0) {
            while (current.compareTo(end) <= 0) {
                intervals.add((T) type.convertFrom(current, DingoConverter.INSTANCE));
                current = adder.apply(current, interval);
                /* if (intervals.size() > 1000000) {
                    throw new IllegalStateException("The generated interval is too large and may fall into an infinite loop.");
                }*/
            }
        } else {
            while (current.compareTo(end) >= 0) {
                intervals.add((T) type.convertFrom(current, DingoConverter.INSTANCE));
                current = adder.apply(current, interval);
                /* if (intervals.size() > 1000000) {
                    throw new IllegalStateException("The generated interval is too large and may fall into an infinite loop.");
                }*/
            }
        }

        return intervals;
    }

    private static void validateNumericParameters(long start, long end, long interval) {
        if (interval == 0) {
            throw new IllegalArgumentException("interval cannot be 0");
        }
        if ((interval > 0 && start > end) || (interval < 0 && start < end)) {
            throw new IllegalArgumentException("Start and end value ranges do not match interval direction");
        }
    }

    private static List<Date> generateDateIntervals(LocalDate start, LocalDate end, Period period) {
        validateDateParameter(start == null, end == null, start.compareTo(end), period);

        List<Date> intervals = new ArrayList<>();
        LocalDate current = start;

        while (period.isNegative() ? current.isAfter(end) : current.isBefore(end)) {
            intervals.add(Date.valueOf(current));
            current = current.plus(period);

        }

        return intervals;
    }

    private static List<Timestamp> generateTimestampIntervals(LocalDateTime start, LocalDateTime end, Period period) {
        validateDateParameter(start == null, end == null, start.compareTo(end), period);

        List<Timestamp> intervals = new ArrayList<>();
        LocalDateTime current = start;

        while (period.isNegative() ? current.isAfter(end) : current.isBefore(end)) {
            intervals.add(Timestamp.valueOf(current));
            current = current.plus(period);
        }

        return intervals;
    }

    private static void validateDateParameter(boolean start, boolean end, int start1, Period period) {
        if (start || end) {
            throw new IllegalArgumentException("Start time and end time cannot be null");
        }
        if (start1 > 0 && !period.isNegative()) {
            throw new IllegalArgumentException("Start time cannot be later than end time");
        }
        if (start1 < 0 && period.isNegative()) {
            throw new IllegalArgumentException("Step is negative, start cannot be less than end");
        }

        if (period.isZero()) {
            throw new IllegalArgumentException("Time interval cannot be 0");
        }
    }

    private static Period getPeriod(IntervalType intervalType) {
        Period period = null;
        if (intervalType instanceof IntervalMonthType.IntervalMonth) {
            IntervalMonthType.IntervalMonth intervalMonth = (IntervalMonthType.IntervalMonth) intervalType;
            int value = intervalMonth.value.intValue();
            period = Period.ofMonths(value);
        } else if (intervalType instanceof IntervalDayType.IntervalDay) {
            IntervalDayType.IntervalDay intervalDay = (IntervalDayType.IntervalDay) intervalType;
            int value = intervalDay.value.intValue();
            period = Period.ofDays(value);
        }
        return period;
    }

    public static List<Object[]> merge(Object[] keys, List<Object> series) {
        List<Object[]> results = new ArrayList<>();
        for (Object obj : series) {
            Object[] tuple = new Object[keys.length + 1];
            tuple[0] = obj;
            System.arraycopy(keys, 0, tuple, 1, keys.length);
            results.add(tuple.clone());
        }
        return results;
    }
}
