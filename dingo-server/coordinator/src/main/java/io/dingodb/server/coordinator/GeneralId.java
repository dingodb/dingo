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

package io.dingodb.server.coordinator;

import io.dingodb.raft.util.BytesUtil;
import io.dingodb.server.coordinator.namespace.Namespace;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.StringJoiner;

@EqualsAndHashCode
public class GeneralId implements Serializable {
    private static final long serialVersionUID = 3355195360067107406L;

    public static final String APP = "app";
    public static final String TABLE = "table";
    public static final String DEFINITION = "definition";
    public static final String META = "meta";
    public static final String VIEW = "view";
    public static final String RESOURCE = "resource";

    public static final String DELIMITER = "_";
    public static final String SEG_DELIMITER = "-";

    public byte[] encode() {
        return BytesUtil.writeUtf8(toString());
    }

    public static GeneralId decode(byte[] bytes) {
        return fromStr(BytesUtil.readUtf8(bytes));
    }

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

    public static byte[] prefix(String namespace) {
        return BytesUtil.writeUtf8(new StringJoiner(SEG_DELIMITER).add(namespace).toString());
    }

    public static byte[] prefix(String namespace, String prefix) {
        return BytesUtil.writeUtf8(new StringJoiner(SEG_DELIMITER).add(namespace).add(prefix).toString());
    }

    public static byte[] appPrefix() {
        return prefix(new StringJoiner(DELIMITER).add(Namespace.DEFAULT).add(APP).toString());
    }

    public static byte[] appPrefix(String prefix) {
        return prefix(new StringJoiner(DELIMITER).add(Namespace.DEFAULT).add(APP).toString(), prefix);
    }

    public static byte[] appViewPrefix() {
        return prefix(new StringJoiner(DELIMITER).add(Namespace.DEFAULT).add(APP).add(VIEW).toString());
    }

    public static byte[] appViewPrefix(String prefix) {
        return prefix(new StringJoiner(DELIMITER).add(Namespace.DEFAULT).add(APP).add(VIEW).toString(), prefix);
    }

    public static byte[] resourceViewPrefix() {
        return prefix(new StringJoiner(DELIMITER).add(Namespace.DEFAULT).add(RESOURCE).add(VIEW).toString());
    }

    public static byte[] resourceViewPrefix(String prefix) {
        return prefix(
            new StringJoiner(DELIMITER).add(Namespace.DEFAULT).add(RESOURCE).add(VIEW).toString(),
            prefix
        );
    }

    public static byte[] tableDefinitionPrefix() {
        return prefix(new StringJoiner(DELIMITER).add(Namespace.DEFAULT).add(TABLE).add(DEFINITION).toString());
    }

    public static byte[] tableMetaPrefix() {
        return prefix(new StringJoiner(DELIMITER).add(Namespace.DEFAULT).add(TABLE).add(META).toString());
    }

    public static GeneralId appOf(long seqNo, String name) {
        return new GeneralId(
            new StringJoiner(DELIMITER).add(Namespace.DEFAULT).add(APP).toString(),
            name,
            seqNo
        );
    }

    public static GeneralId appViewOf(long seqNo, String name) {
        return new GeneralId(
            new StringJoiner(DELIMITER).add(Namespace.DEFAULT).add(APP).add(VIEW).toString(),
            name,
            seqNo
        );
    }

    public static GeneralId resourceViewOf(long seqNo, String name) {
        return new GeneralId(
            new StringJoiner(DELIMITER).add(Namespace.DEFAULT).add(RESOURCE).add(VIEW).toString(),
            name,
            seqNo
        );
    }

    public static GeneralId tableDefinitionOf(long seqNo, String name) {
        return new GeneralId(
            new StringJoiner(DELIMITER).add(Namespace.DEFAULT).add(TABLE).add(DEFINITION).toString(),
            name,
            seqNo
        );
    }

    public static GeneralId tableMetaOf(long seqNo, String name) {
        return new GeneralId(
            new StringJoiner(DELIMITER).add(Namespace.DEFAULT).add(TABLE).add(META).toString(),
            name,
            seqNo
        );
    }
}
