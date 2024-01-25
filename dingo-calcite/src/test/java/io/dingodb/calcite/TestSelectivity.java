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

package io.dingodb.calcite;

import io.dingodb.calcite.stats.CountMinSketch;
import io.dingodb.calcite.stats.Histogram;
import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSelectivity {
    static Iterator<Integer> iterator;

    @BeforeAll
    public static void setupAll() {
        Stream<Integer> stream = Stream.generate(() -> new Random().nextInt(100000)).limit(100000);
        iterator = stream.iterator();
    }

    @Test
    public void test254BucketsHistogram() {
        List<Integer> data = new ArrayList<>();
        long lessThan = 0;
        long equals = 0;
        Histogram histogram = new Histogram("dingo", "demo", "id", null, 0);
        while (iterator.hasNext()) {
            int val = iterator.next();
            data.add(val);
            histogram.setRegionMax(val);
            histogram.setRegionMin(val);
            if (val < 10000) {
                lessThan++;
            } else if (val == 10000) {
                equals++;
            }
        }

        histogram.init(254);
        for (Integer val : data) {
            histogram.addValue(val);
        }
        double ltSelectivity = histogram.estimateSelectivity(SqlKind.LESS_THAN, 10000);
        BigDecimal filter = new BigDecimal(lessThan);

        BigDecimal realLtSelectivity = filter.divide(new BigDecimal(data.size()));
        assertTrue(() -> Math.abs(ltSelectivity - realLtSelectivity.doubleValue()) < 0.10);

        double gtSelectivity = histogram.estimateSelectivity(SqlKind.GREATER_THAN_OR_EQUAL, 10000);
        filter = new BigDecimal(data.size() - lessThan);
        BigDecimal realGtSelectivity = filter.divide(new BigDecimal(data.size()));
        assertTrue(() -> Math.abs(gtSelectivity - realGtSelectivity.doubleValue()) < 0.10);

        double eqSelectivity = histogram.estimateSelectivity(SqlKind.EQUALS, 10000);
        filter = new BigDecimal(equals);
        BigDecimal realEqSelectivity = filter.divide(new BigDecimal(data.size()));
        assertTrue(() -> Math.abs(eqSelectivity - realEqSelectivity.doubleValue()) < 0.10);
    }

    @Test
    public void testCmSketch() {
        CountMinSketch countMinSketch = new CountMinSketch("dingo", "demo", "id", 0, 3, 5);
        String val = "zhangsan";
        String[] sample = new String[]{"zhangsan", "lisi", "wangwu", "zhaoliu", "laob"};
        String str;
        long count = 0;
        Random random = new Random();
        for (int i = 0; i < 1000000; i++) {
            str = sample[random.nextInt(4)];
            if (str.equals(val)) {
                count++;
            }
            countMinSketch.setString(str);
        }
        int estimate = countMinSketch.getEstimatedCountString(val);
        BigDecimal total = new BigDecimal(1000000);
        BigDecimal estimateSelectivity = new BigDecimal(estimate).divide(total);
        BigDecimal realSelectivity = new BigDecimal(count).divide(total);
        assertTrue(() -> Math.abs(estimateSelectivity.doubleValue() - realSelectivity.doubleValue()) < 0.10);
    }

}
