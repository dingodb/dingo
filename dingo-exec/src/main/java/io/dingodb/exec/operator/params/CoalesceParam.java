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

package io.dingodb.exec.operator.params;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dingodb.common.profile.Profile;
import io.dingodb.exec.dag.Vertex;
import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

@Getter
@JsonTypeName("coalesce")
@JsonPropertyOrder({"inputNum"})
public class CoalesceParam extends AbstractParams {

    @JsonProperty("inputNum")
    private final int inputNum;

    private transient boolean[] finFlags;
    private transient List<Profile> profiles;

    @Getter
    @Setter
    private String lastFin;

    public CoalesceParam(int inputNum) {
        this.inputNum = inputNum;
    }

    @Override
    public void init(Vertex vertex) {
        profiles = new LinkedList<>();
        finFlags = new boolean[inputNum];
    }

    @Override
    public void setParas(Object[] paras) {
        Arrays.fill(finFlags, false);
        super.setParas(paras);
    }

    public synchronized void addProfiles(List<Profile> profiles) {
        if (this.profiles == null) {
            this.profiles = new LinkedList<>();
        }
        this.profiles.addAll(profiles);
    }

    public void setFinFlags(int pin) {
        finFlags[pin] = true;
    }

    public void clear() {
        if (profiles != null) {
            profiles.clear();
        }
    }

}
