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

package io.dingodb.coordinator.score.impl;

import io.dingodb.common.util.Optional;
import io.dingodb.common.util.PreParameters;
import io.dingodb.coordinator.score.Score;
import lombok.Builder;

import java.math.BigDecimal;

public class SimplePercentScore implements Score<SimplePercentScore, BigDecimal, Number> {

    private static final BigDecimal MAX = new BigDecimal(100);
    private static final BigDecimal MIN = new BigDecimal(0);

    private final Number total;

    private BigDecimal score = MIN;

    @Builder
    private SimplePercentScore(Number total) {
        this.total = total;
    }

    public static SimplePercentScore of(Number total) {
        if (total == null) {
            throw new IllegalArgumentException("Total must nonnull.");
        }
        return new SimplePercentScore(total);
    }

    public static SimplePercentScore of(Number total, Number value) {
        SimplePercentScore score = of(total);
        if (value != null) {
            score.update(value);
        }
        return score;
    }

    @Override
    public int update(Number value) {
        BigDecimal oldScore = score();
        this.score = BigDecimal.valueOf(value.doubleValue() / this.total.doubleValue()).multiply(MAX);
        return score.compareTo(oldScore);
    }

    @Override
    public BigDecimal score() {
        return PreParameters.cleanNull(score, MIN);
    }

    @Override
    public BigDecimal max() {
        return MAX;
    }

    @Override
    public BigDecimal min() {
        return MIN;
    }

    @Override
    public int compareTo(SimplePercentScore other) {
        return score().compareTo(Optional.ofNullable(other).map(SimplePercentScore::score).orElseGet(this::min));
    }
}
