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

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class ExecProfile extends Profile {
    private Profile profile;
    private transient Object[] lastTuple;

    public ExecProfile(String type) {
        super(type);
        start();
    }

    public void increment() {
        this.count ++;
    }

    public String dumpTree(byte[] prefix) {
        StringBuilder dag = new StringBuilder();
        dag.append(this).append("\r\n");

        if (profile != null) {
            byte[] prefix1 = new byte[prefix.length + 2];
            System.arraycopy(space, 0, prefix1, 0, 2);
            System.arraycopy(prefix, 0, prefix1, 2, prefix.length);
            dag.append(profile.dumpTree(profile, prefix1));
        }
        return dag.toString();
    }

    @Override
    public String toString() {
        return "ExecProfile{" +
            "duration=" + duration +
            ", start=" + start +
            ", end=" + end +
            '}';
    }

    public void clear() {
        super.clear();
        if (this.profile != null) {
            this.profile.clear();
        }
    }
}
