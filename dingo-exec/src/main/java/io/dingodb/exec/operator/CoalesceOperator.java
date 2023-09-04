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
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.FinWithProfiles;
import io.dingodb.exec.fin.OperatorProfile;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

@Slf4j
@JsonTypeName("coalesce")
@JsonPropertyOrder({"inputNum", "output"})
public final class CoalesceOperator extends SoleOutOperator {
    @JsonProperty("inputNum")
    private final int inputNum;

    private final List<OperatorProfile> profiles = new LinkedList<>();
    private boolean[] finFlags;

    @JsonCreator
    public CoalesceOperator(
        @JsonProperty("inputNum") int inputNum
    ) {
        this.inputNum = inputNum;
    }

    @Override
    public void init() {
        super.init();
        finFlags = new boolean[inputNum];
    }

    @Override
    public synchronized boolean push(int pin, Object[] tuple) {
        if (log.isDebugEnabled()) {
            log.debug("Got tuple from pin {}.", pin);
        }
        return output.push(tuple);
    }

    @Override
    public synchronized void fin(int pin, Fin fin) {
        if (log.isDebugEnabled()) {
            log.debug("Got FIN from pin {}.", pin);
        }
        if (fin instanceof FinWithException) {
            output.fin(fin);
            return;
        }
        setFin(pin, fin);
        if (isAllFin()) {
            output.fin(new FinWithProfiles(profiles));
            profiles.clear();
        }
    }

    private void setFin(int pin, Fin fin) {
        assert pin < inputNum : "Pin no is greater than the max (" + inputNum + ").";
        assert !finFlags[pin] : "Input on pin (" + pin + ") is already finished.";
        finFlags[pin] = true;
        if (fin instanceof FinWithProfiles) {
            profiles.addAll(((FinWithProfiles) fin).getProfiles());
        }
    }

    private boolean isAllFin() {
        for (boolean b : finFlags) {
            if (!b) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void setParas(Object[] paras) {
        Arrays.fill(finFlags, false);
        super.setParas(paras);
    }
}
