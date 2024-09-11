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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Time;
import java.util.Base64;
import java.util.List;

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

    public void traceTree(byte[] prefix, List<Object[]> rowList) {
        String prefixStr;
        try {
            prefixStr = new String(prefix, "GBK");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        Object[] val = new Object[3];
        val[0] = prefixStr + "runStmt";
        val[1] = DateTimeUtils.timeFormat(new Time(start));
        val[2] = String.valueOf(duration);
        rowList.add(val);

        if (profile != null) {
            byte[] prefix1 = new byte[prefix.length + 2];
            System.arraycopy(space, 0, prefix1, 0, 2);
            System.arraycopy(prefix, 0, prefix1, 2, prefix.length);
            profile.traceTree(profile, prefix1, rowList);
        }
    }

    public String binaryPlanOp() {
        if (profile != null) {
            ObjectMapper mapper = new ObjectMapper();
            try {
                return mapper.writeValueAsString(profile);
            } catch (IOException e) {
                return "";
            }
        }
        return "";
    }

    @Override
    public String toString() {
        return "Exec{" +
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
