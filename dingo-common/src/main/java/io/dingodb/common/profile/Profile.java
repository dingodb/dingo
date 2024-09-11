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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dingodb.common.util.ByteUtils;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.UnsupportedEncodingException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Data
public class Profile {
    @JsonProperty("type")
    String type;
    @JsonProperty("start")
    long start;
    @JsonProperty("end")
    long end;
    @JsonProperty("count")
    long count;
    @JsonProperty("duration")
    long duration;
    @JsonProperty("max")
    long max;
    @JsonProperty("min")
    long min;
    @JsonProperty("avg")
    long avg;

    @JsonProperty("children")
    List<Profile> children;
    @JsonProperty("autoIncId")
    long autoIncId;
    @JsonProperty("hasAutoInc")
    boolean hasAutoInc;

    @JsonProperty("location")
    String location = "";
    @JsonIgnore
    StringBuilder dagText;
    @JsonIgnore
    final byte[] terminated = ByteUtils.hexStringToByteArray("a9b8a9a4");
    @JsonIgnore
    final byte[] space = new byte[]{0x20, 0x20};

    public Profile() {
    }

    public Profile(String type) {
        this.type = type;
        this.children = new ArrayList<>();
        dagText = new StringBuilder();
    }

    public void start() {
        this.start = System.currentTimeMillis();
    }

    public void end() {
        this.end = System.currentTimeMillis();
        if (start > 0) {
            this.duration = end - start;
            if (count > 0) {
                this.avg = this.duration / count;
                this.avg = Math.min(avg, max);
            }
        }
    }

    public long getDuration() {
        if (duration == 0 && end > start) {
            duration = end - start;
        }
        return duration;
    }

    public void traceTree(Profile profile, byte[] prefix, List<Object[]> rowList) {
        String node;
        try {
            node = new String(prefix, "GBK");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        boolean skip = true;
        if (!"base".equals(profile.type)
            && !"source".equals(profile.type)
            && !"root".equals(profile.type) && profile.getEnd() > 0 && profile.getStart() > 0
        ) {
            Object[] val = new Object[3];
            val[0] = node + profile.type;
            if (profile instanceof SourceProfile) {
                SourceProfile sourceProfile = (SourceProfile) profile;
                if (sourceProfile.regionId > 0) {
                    val[0] = val[0] + ", regionId:" + sourceProfile.regionId
                        + ",taskType:" + sourceProfile.getTaskType();
                }
            }
            val[1] = DateTimeUtils.timeFormat(new Time(profile.start));
            val[2] = String.valueOf(profile.getDuration());
            rowList.add(val);
            skip = false;
        }
        for (Profile child : profile.children) {
            if (child != null) {
                if (skip) {
                    traceTree(child, prefix, rowList);
                } else {
                    byte[] prefix1 = new byte[prefix.length + 2];
                    System.arraycopy(space, 0, prefix1, 0, 2);
                    System.arraycopy(prefix, 0, prefix1, 2, prefix.length);
                    traceTree(child, prefix1, rowList);
                }
            }
        }
    }

    public String dumpTree(Profile profile, byte[] prefix) {
        String node;
        try {
            node = new String(prefix, "GBK");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        if (!"base".equals(profile.type)) {
            dagText.append(node).append(profile.type)
                .append(",duration:").append(profile.getDuration())
                .append(",count:").append(profile.count)
                .append(",start:").append(profile.start)
                .append(",end:").append(profile.end)
                .append("\r\n");
                //.append("  ").append(profile.location).append("\r\n");
        }
        for (Profile child : profile.children) {
            byte[] prefix1 = new byte[prefix.length + 2];
            System.arraycopy(space, 0, prefix1, 0, 2);
            System.arraycopy(prefix, 0, prefix1, 2, prefix.length);
            if (child != null) {
                dumpTree(child, prefix1);
            } else {
                //log.info("child is null:" + profile);
            }
        }

        return dagText.toString();
    }

    public String detail() {
        return "type " + type + ":"
           // + " Start: " + dateFormat.format(new Date(start))
           // + " End: " + dateFormat.format(new Date(end))
            + " Duration: " + duration + "ms"
            + " Count: " + count;
    }

    public void clear() {
        this.dagText = new StringBuilder();
        this.count = 0;
        if (this.children != null) {
            this.children.clear();
        }
        this.start = System.currentTimeMillis();
        this.end = 0;
        this.count = 0;
        this.duration = 0;
        this.max = 0;
        this.min = 0;
        this.avg = 0;
        this.hasAutoInc = false;
        this.autoIncId = 0;
        this.location = "";
    }

}
