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

package io.dingodb.sdk.common;

import io.dingodb.sdk.compute.Executive;
import io.dingodb.sdk.compute.executive.AddExecutive;
import io.dingodb.sdk.compute.executive.AppendExecutive;
import io.dingodb.sdk.compute.executive.CountExecutive;
import io.dingodb.sdk.compute.executive.MaxExecutive;
import io.dingodb.sdk.compute.executive.MinExecutive;
import io.dingodb.sdk.compute.executive.ReplaceExecutive;
import io.dingodb.sdk.compute.executive.SumExecutive;

import java.io.Serializable;

public class Operation implements Serializable {

    private static final long serialVersionUID = 8603290191302531535L;

    public final Type type;
    public final Column[] columns;

    public Operation(Type type, Column[] columns) {
        this.type = type;
        this.columns = columns;
    }

    public static Operation add(Column... columns) {
        return new Operation(Type.ADD, columns);
    }

    public static Operation max(Column... columns) {
        return new Operation(Type.MAX, columns);
    }

    public static Operation min(Column... columns) {
        return new Operation(Type.MIN, columns);
    }

    public static Operation sum(Column... columns) {
        return new Operation(Type.SUM, columns);
    }

    public static Operation count(Column... columns) {
        return new Operation(Type.COUNT, columns);
    }

    public static Operation append(Column... columns) {
        return new Operation(Type.APPEND, columns);
    }

    public static Operation replace(Column... columns) {
        return new Operation(Type.REPLACE, columns);
    }

    public static enum Type {
        ADD(new AddExecutive(), true),
        MAX(new MaxExecutive(), false),
        MIN(new MinExecutive(), false),
        SUM(new SumExecutive(), false),
        COUNT(new CountExecutive(), false),

        APPEND(new AppendExecutive(), true),
        REPLACE(new ReplaceExecutive(), true),
        ;

        public final transient Executive executive;
        public final boolean isWrite;

        Type(Executive<?, ?> executive, boolean isWrite) {
            this.executive = executive;
            this.isWrite = isWrite;
        }
    }

}
