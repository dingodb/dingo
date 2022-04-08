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

import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.fin.FinWithProfiles;
import io.dingodb.exec.fin.OperatorProfile;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Source operator has no inputs and only one output.
 */
@Slf4j
public abstract class SourceOperator extends SoleOutOperator {
    protected final List<OperatorProfile> profiles = new LinkedList<>();

    @Override
    public void init() {
        super.init();
        OperatorProfile profile = new OperatorProfile();
        profile.setOperatorId(id);
        profiles.add(profile);
    }

    public abstract boolean push();

    @Override
    public synchronized boolean push(int pin, @Nonnull Object[] tuple) {
        return push();
    }

    @Override
    public synchronized void fin(int pin, Fin fin) {
        if (fin != null && fin instanceof FinWithException) {
            output.fin(fin);
        } else {
            output.fin(new FinWithProfiles(profiles));
        }
    }

    public OperatorProfile getProfile() {
        return profiles.get(0);
    }
}
