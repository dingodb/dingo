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
import java.util.Arrays;

@EqualsAndHashCode(callSuper = true)
@Data
public class SqlProfile extends Profile {
    private String instance;
    private String schema;
    private String sql;
    private String plan;
    private String simpleUser;
    private boolean prepared;
    private String statementType;

    private PlanProfile planProfile;

    private ExecProfile execProfile;

    private CommitProfile commitProfile;

    public SqlProfile(String type, boolean prepared) {
        super(type);
        start();
        this.prepared = prepared;
    }

    public void setPlanProfile(PlanProfile planProfile) {
        if (planProfile != null && planProfile.end == 0) {
            planProfile.end();
            this.statementType = planProfile.getStmtType();
        }
        this.planProfile = planProfile;
    }

    public void setExecProfile(ExecProfile execProfile) {
        if (execProfile != null) {
            execProfile.end();
            this.execProfile = execProfile;
        }
    }

    public void setCommitProfile(CommitProfile commitProfile) {
        if (commitProfile != null && commitProfile.start > 0) {
            commitProfile.end();
        }
        this.commitProfile = commitProfile;
    }

    public String summaryKey() {
        if (schema == null) {
            schema = "null";
        }
        return type + schema + ":" + sql;
    }

    public String dumpTree() {
        String termStr;
        byte[] prefix = new byte[terminated.length + 2];
        System.arraycopy(space, 0, prefix, 0, 2);
        System.arraycopy(terminated, 0, prefix, 2, terminated.length);
        try {
            termStr = new String(prefix, "GBK");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        StringBuilder dag = new StringBuilder();
        dag.append("\r\n").append(this).append("\r\n");
        if (planProfile != null) {
            dag.append(termStr).append(planProfile.dumpTree(prefix)).append("\r\n");
        }
        if (execProfile != null) {
            dag.append(termStr).append(execProfile.dumpTree(prefix)).append("\r\n");
        }
        if (commitProfile != null) {
            dag.append(termStr).append(commitProfile.dumpTree(prefix)).append("\r\n");
        }
        return dag.toString();
    }

    @Override
    public String toString() {
        return "SqlProfile{" +
            "schema='" + schema + '\'' +
            ", sql='" + sql + '\'' +
            ", duration=" + duration +
            ", start=" + start +
            ", end=" + end +
//            ", planningProfile=" + planProfile +
//            ", execProfile=" + execProfile +
//            ", commitProfile=" + commitProfile +
            '}';
    }
}
