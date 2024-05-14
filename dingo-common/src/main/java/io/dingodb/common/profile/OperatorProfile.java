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

package io.dingodb.common.profile;

import io.dingodb.common.config.DingoConfiguration;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class OperatorProfile extends Profile {

    public OperatorProfile(String type) {
        super(type);
        this.location = DingoConfiguration.location().url();
    }

    @Override
    public void end() {
        this.end = System.currentTimeMillis();
        if (duration > 0 && count > 0) {
            this.avg = duration / count;
        }
    }

    public void time(long start) {
        incrTime(start);
        count ++;
    }

    public void incrTime(long start) {
        long current = System.currentTimeMillis();
        long time = current - start;
        duration += time;
        if (time > max) {
            max = time;
        }
        if (time < min) {
            min = time;
        }
    }

    public void decreaseCount() {
        count --;
    }

    public void mergeChild() {
        if (this.children != null && this.children.size() > 1) {
            boolean res = this.children.stream().anyMatch(profile -> profile.duration == 0 && profile.getChildren().isEmpty());
            if (!res) {
                return;
            }
            Profile first = this.children.get(0);
            for (int i = 1; i < this.children.size(); i ++) {
                Profile profile = this.children.get(i);
                first.count += profile.count;
            }
            this.children.clear();
            this.children.add(first);
        }
    }

    @Override
    public String toString() {
        return "{" +
            "type='" + type + '\'' +
            ", start=" + start +
            ", end=" + end +
            ", count=" + count +
            ", duration=" + duration +
            ", max=" + max +
            ", min=" + min +
            ", avg=" + avg +
//            ", children=" + children +
            '}';
    }
}
