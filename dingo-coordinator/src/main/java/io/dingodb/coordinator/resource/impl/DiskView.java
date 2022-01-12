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

package io.dingodb.coordinator.resource.impl;

import io.dingodb.coordinator.GeneralId;
import io.dingodb.coordinator.resource.ResourceView;
import io.dingodb.coordinator.score.impl.SimplePercentScore;
import io.dingodb.meta.Location;

import java.util.Map;

public class DiskView implements ResourceView {

    private final GeneralId id;

    private final SimplePercentScore score;

    private long capacity;
    private long usable;

    private Map<String, String> labels;

    public DiskView(GeneralId id, long capacity) {
        this.id = id;
        this.score = SimplePercentScore.of(capacity);
    }

    @Override
    public GeneralId resourceId() {
        return id;
    }

    @Override
    public Map<String, String> labels() {
        return labels;
    }

    public void refreshUsable(long usable) {
        score.update(usable);
    }

    @Override
    public SimplePercentScore score() {
        return score;
    }

    @Override
    public Location location() {
        return null;
    }
}
