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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Arrays;
import javax.annotation.Nonnull;

@JsonTypeName("sumUp")
@JsonPropertyOrder({"inputNum"})
public class SumUpOperator extends SoleOutMultiInputOperator {
    private long sum;
    private boolean[] finFlag;

    @JsonCreator
    public SumUpOperator(
        @JsonProperty("inputNum") int inputNum
    ) {
        super(inputNum);
    }

    @Override
    public void init() {
        super.init();
        sum = 0;
    }

    @Override
    public synchronized boolean push(int pin, @Nonnull Object[] tuple) {
        if (!Arrays.equals(tuple, FIN)) {
            sum += (long) tuple[0];
            return true;
        }
        setFin(pin);
        if (isAllFin()) {
            if (pushOutput(new Object[]{sum})) {
                pushOutput(FIN);
            }
            return false;
        }
        return true;
    }
}
