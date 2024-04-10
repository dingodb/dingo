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

import java.io.UnsupportedEncodingException;

@Data
@EqualsAndHashCode(callSuper = true)
public class CommitProfile extends Profile {
    public CommitProfile() {
        super("commit");
    }

    private long preWritePrimary;
    private long preWritePrimaryTime;
    private long preWriteSecond;
    private long preWriteSecondTime;
    private long commitPrimary;
    private long commitPrimaryTime;
    private long commitSecond;
    private long commitSecondTime;
    private long clean;
    private long cleanTime;

    public void endPreWritePrimary() {
        this.preWritePrimaryTime = System.currentTimeMillis();
        preWritePrimary = preWritePrimaryTime - start;
    }

    public void endPreWriteSecond() {
        this.preWriteSecondTime = System.currentTimeMillis();
        preWriteSecond = preWriteSecondTime - preWritePrimaryTime;
    }

    public void endCommitPrimary() {
        this.commitPrimaryTime = System.currentTimeMillis();
        commitPrimary = commitPrimaryTime - preWriteSecondTime;
    }

    public void endCommitSecond() {
        this.commitSecondTime = System.currentTimeMillis();
        commitSecond = commitSecondTime - commitPrimaryTime;
    }

    public void endClean() {
        if (start > 0) {
            this.cleanTime = System.currentTimeMillis();
            if (commitSecondTime > 0) {
                clean = cleanTime - commitSecondTime;
            } else if (commitPrimaryTime > 0) {
                clean = cleanTime - commitPrimaryTime;
            }
            end();
        }
    }

    public long getPreWrite() {
        return preWritePrimary + preWriteSecond;
    }

    public long getCommit() {
        return commitPrimary + commitSecond;
    }

    public synchronized void reset() {
        this.start = 0;
        this.end = 0;
        this.duration = 0;
        this.preWriteSecond = 0;
        this.preWriteSecondTime = 0;
        this.preWritePrimaryTime = 0;
        this.preWritePrimary = 0;
        this.commitPrimary = 0;
        this.commitPrimaryTime = 0;
        this.commitSecond = 0;
        this.commitSecondTime = 0;
        this.clean = 0;
        this.cleanTime = 0;
    }

    public String dumpTree(byte[] prefix) {
        StringBuilder dag = new StringBuilder();
        dag.append(this).append("\r\n");

        byte[] prefix1 = new byte[prefix.length + 2];
        System.arraycopy(space, 0, prefix1, 0, 2);
        System.arraycopy(prefix, 0, prefix1, 2, prefix.length);
        String termStr;
        try {
            termStr = new String(prefix1, "GBK");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        dag.append(termStr).append("preWritePrimary:").append(preWritePrimary).append("\r\n");
        dag.append(termStr).append("preWriteSecond:").append(preWriteSecond).append("\r\n");
        dag.append(termStr).append("commitPrimary:").append(commitPrimary).append("\r\n");
        dag.append(termStr).append("commitSecond:").append(commitSecond).append("\r\n");
        dag.append(termStr).append("clean:").append(clean).append("\r\n");
        return dag.toString();
    }

    @Override
    public String toString() {
        return "CommitProfile{" +
            "duration=" + duration +
            ", start=" + start +
            ", end=" + end +
            '}';
    }
}
