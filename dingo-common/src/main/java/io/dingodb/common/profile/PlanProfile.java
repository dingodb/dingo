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

import io.dingodb.expr.runtime.utils.DateTimeUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.UnsupportedEncodingException;
import java.sql.Time;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
public class PlanProfile extends Profile {
    public PlanProfile() {
        super("planning");
        this.start = System.currentTimeMillis();
    }

    private String stmtType;
    private long parse;
    private long parseTime;
    private long validate;
    private long validateTime;
    private long optimize;
    private long optimizeTime;
    private long lock;
    private long lockTime;
    private boolean hitCache;
    private List<String> tableList;

    public void endParse() {
        this.parseTime = System.currentTimeMillis();
        this.parse = parseTime - start;
    }

    public void endValidator() {
        this.validateTime = System.currentTimeMillis();
        this.validate = validateTime - parseTime;
    }

    public void endOptimize() {
        this.optimizeTime = System.currentTimeMillis();
        this.optimize = optimizeTime - validateTime;
    }

    public void endLock() {
        this.lockTime = System.currentTimeMillis();
        this.lock = lockTime - optimizeTime;
    }

    public void traceTree(byte[] prefix, List<Object[]> rowList) {
        String termStr;
        try {
            termStr = new String(prefix, "GBK");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        Object[] val = new Object[3];
        val[0] = termStr + "compile";
        val[1] = DateTimeUtils.timeFormat(new Time(start));
        val[2] = String.valueOf(duration);
        rowList.add(val);

        byte[] prefix1 = new byte[prefix.length + 2];
        System.arraycopy(space, 0, prefix1, 0, 2);
        System.arraycopy(prefix, 0, prefix1, 2, prefix.length);
        try {
            termStr = new String(prefix1, "GBK");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        Object[] parseVal = new Object[3];
        parseVal[0] = termStr + "parse";
        parseVal[1] = DateTimeUtils.timeFormat(new Time(start));
        parseVal[2] = String.valueOf(parse);
        rowList.add(parseVal);

        Object[] validateVal = new Object[3];
        validateVal[0] = termStr + "validate";
        validateVal[1] = DateTimeUtils.timeFormat(new Time(parseTime));
        validateVal[2] = String.valueOf(validate);
        rowList.add(validateVal);

        Object[] optimizeVal = new Object[3];
        optimizeVal[0] = termStr + "optimize";
        optimizeVal[1] = DateTimeUtils.timeFormat(new Time(validateTime));
        optimizeVal[2] = String.valueOf(optimize);
        rowList.add(optimizeVal);
    }

    public String dumpTree(byte[] prefix) {
        StringBuilder planDag = new StringBuilder();
        planDag.append(this).append("\r\n");

        byte[] prefix1 = new byte[prefix.length + 2];
        System.arraycopy(space, 0, prefix1, 0, 2);
        System.arraycopy(prefix, 0, prefix1, 2, prefix.length);
        String termStr;
        try {
            termStr = new String(prefix1, "GBK");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        planDag.append(termStr).append("parse:").append(parse).append("\r\n");
        planDag.append(termStr).append("validate:").append(validate).append("\r\n");
        planDag.append(termStr).append("optimize:").append(optimize).append("\r\n");
        planDag.append(termStr).append("lock:").append(lock).append("\r\n");
        return planDag.toString();
    }

    @Override
    public String toString() {
        return "PlanProfile{" +
            "duration=" + duration +
            ", start=" + start +
            ", end=" + end +
            '}';
    }
}
