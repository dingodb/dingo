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

import io.dingodb.common.util.ByteUtils;
import lombok.Data;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

@Data
public class Profile {
    String type;
    long start;
    long end;
    long count;
    long duration;
    long max;
    long min;
    long avg;
    int dagLevel;
    List<Profile> children;
    long autoIncId;
    boolean hasAutoInc;
    StringBuilder dagText;
    final byte[] terminated = ByteUtils.hexStringToByteArray("a9b8a9a4");
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

    public String dumpTree(Profile profile, byte[] prefix) {
        String node;
        try {
            node = new String(prefix, "GBK");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        dagText.append(node).append("operator:").append(profile.type)
            .append(",duration:").append(profile.duration)
            .append(",count:").append(profile.count).append("\r\n");
        for (Profile child : profile.children) {
            byte[] prefix1 = new byte[prefix.length + 2];
            System.arraycopy(space, 0, prefix1, 0, 2);
            System.arraycopy(prefix, 0, prefix1, 2, prefix.length);
            dumpTree(child, prefix1);
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
}
