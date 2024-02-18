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

import io.dingodb.common.CommonId;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.OperatorProfile;
import io.dingodb.exec.table.Part;
import lombok.Getter;

import java.util.LinkedList;
import java.util.List;

public abstract class SourceParam extends AbstractParams {

    @Getter
    protected transient List<OperatorProfile> profiles;

    public SourceParam() {
    }

    public SourceParam(CommonId partId, Part part) {
        super(partId, part);
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        profiles = new LinkedList<>();
    }

    public void clear() {
        if (profiles != null) {
            profiles.clear();
        }
    }

    public synchronized OperatorProfile getProfile(CommonId id) {
        if (profiles == null) {
            profiles = new LinkedList<>();
        }
        OperatorProfile profile = new OperatorProfile();
        profile.setOperatorId(id);
        profiles.add(profile);
        return profile;
    }
}
