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

import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nonnull;

@JsonTypeName("coalesce")
@JsonPropertyOrder({"inputNum", "output"})
public final class CoalesceOperator extends SoleOutOperator {
    @JsonProperty("inputNum")
    private final int inputNum;

    private final List<OperatorProfile> profiles = new LinkedList<>();
    private boolean[] finFlag;

    @JsonCreator
    public CoalesceOperator(
        @JsonProperty("inputNum") int inputNum
    ) {
        this.inputNum = inputNum;
    }

    @Override
    public void init() {
        super.init();
        finFlag = new boolean[inputNum];
    }

    private void setFin(int pin, Fin fin) {
        assert pin < inputNum : "Pin no is greater than the max (" + inputNum + ").";
        assert !finFlag[pin] : "Input on pin (" + pin + ") is already finished.";
        finFlag[pin] = true;
        if (fin instanceof FinWithProfiles) {
            profiles.addAll(((FinWithProfiles) fin).getProfiles());
        }
    }

    private boolean isAllFin() {
        for (boolean b : finFlag) {
            if (!b) {
                return false;
            }
        }
        return true;
    }

    @Override
    public synchronized boolean push(int pin, @Nonnull Object[] tuple) {
        return output.push(tuple);
    }

    @Override
    public synchronized void fin(int pin, Fin fin) {
        if (fin instanceof FinWithException) {
            output.fin(fin);
            return;
        }

        setFin(pin, fin);
        if (isAllFin()) {
            output.fin(new FinWithProfiles(profiles));
        }
    }
}
