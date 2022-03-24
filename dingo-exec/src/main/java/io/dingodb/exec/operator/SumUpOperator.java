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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.exec.fin.Fin;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

@JsonTypeName("sumUp")
@JsonPropertyOrder({"inputNum"})
@Slf4j
public class SumUpOperator extends SoleOutOperator {
    private long sum;

    @Override
    public void init() {
        super.init();
        sum = 0;
    }

    @Override
    public synchronized boolean push(int pin, @Nonnull Object[] tuple) {
        sum += (long) tuple[0];
        return true;
    }

    @Override
    public synchronized void fin(int pin, Fin fin) {
        output.push(new Object[]{sum});
        output.fin(fin);
    }
}
