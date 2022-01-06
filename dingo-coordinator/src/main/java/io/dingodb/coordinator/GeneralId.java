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

package io.dingodb.coordinator;

import io.dingodb.coordinator.namespace.Namespace;
import lombok.EqualsAndHashCode;

import java.util.StringJoiner;

import static io.dingodb.coordinator.namespace.Namespace.DEFAULT;

@EqualsAndHashCode
public class GeneralId {
    public static final String APP = "app";
    public static final String VIEW = "view";
    public static final String RESOURCE = "resource";

    public static final String DELIMITER = "_";
    public static final String SEG_DELIMITER = "-";

    private final Long seqNo;

    // todo need validation name
    private final String name;

    // todo need validation namespace
    private final String namespace;

    private final String str;

    public GeneralId(
        String namespace,
        String name,
        Long seqNo
    ) {
        this.seqNo = seqNo;
        this.name = name;
        this.namespace = namespace;
        this.str = new StringJoiner(SEG_DELIMITER).add(namespace).add(name).add(seqNo.toString()).toString();
    }

    public Long seqNo() {
        return seqNo;
    }

    public String name() {
        return name;
    }

    public String namespace() {
        return namespace;
    }

    @Override
    public String toString() {
        return str;
    }

    public static GeneralId fromStr(String str) {
        String[] parts = str.split(SEG_DELIMITER);
        return new GeneralId(parts[0], parts[1], Long.parseLong(parts[2]));
    }

    public static GeneralId appOf(long seqNo, String name) {
        return new GeneralId(new StringJoiner(DELIMITER).add(DEFAULT).add(APP).toString(), name, seqNo);
    }

    public static GeneralId appViewOf(long seqNo, String name) {
        return new GeneralId(new StringJoiner(DELIMITER).add(DEFAULT).add(APP).add(VIEW).toString(), name, seqNo);
    }

    public static GeneralId resourceViewOf(long seqNo, String name) {
        return new GeneralId(new StringJoiner(DELIMITER).add(DEFAULT).add(RESOURCE).add(VIEW).toString(), name, seqNo);
    }
}
