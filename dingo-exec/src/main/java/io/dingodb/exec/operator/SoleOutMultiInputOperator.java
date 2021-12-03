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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithProfiles;
import io.dingodb.exec.fin.OperatorProfile;
import lombok.RequiredArgsConstructor;

import java.util.LinkedList;
import java.util.List;

@RequiredArgsConstructor
public abstract class SoleOutMultiInputOperator extends SoleOutOperator {
    private final List<OperatorProfile> profiles = new LinkedList<>();

    @JsonProperty("inputNum")
    private final int inputNum;
    private boolean[] finFlag;

    @Override
    public void init() {
        super.init();
        finFlag = new boolean[inputNum];
    }

    protected void setFin(int pin, Fin fin) {
        assert pin < inputNum : "Pin no is greater than the max (" + inputNum + ").";
        assert !finFlag[pin] : "Input on pin (" + pin + ") is already finished.";
        finFlag[pin] = true;
        if (fin instanceof FinWithProfiles) {
            profiles.addAll(((FinWithProfiles) fin).getProfiles());
        }
    }

    protected boolean isAllFin() {
        for (boolean b : finFlag) {
            if (!b) {
                return false;
            }
        }
        return true;
    }

    protected void outputFin() {
        output.fin(new FinWithProfiles(profiles));
    }
}
